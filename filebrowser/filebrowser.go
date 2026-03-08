package filebrowser

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"
)

const (
	defaultFileBrowserURL  = "http://localhost:10001"
	defaultFileBrowserUser = "admin"
)

type Health struct {
	Running bool   `json:"running"`
	Version string `json:"version,omitempty"`
	URL     string `json:"url"`
}

type pm2Process struct {
	Name   string `json:"name"`
	PM2Env struct {
		Status     string   `json:"status"`
		PmExecPath string   `json:"pm_exec_path"`
		Args       []string `json:"args"`
	} `json:"pm2_env"`
}

func EnsureEnvFile(path string) error {
	if _, err := os.Stat(path); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}

	content := strings.Join([]string{
		"FILEBROWSER_URL=http://localhost:10001",
		"FILEBROWSER_ADMIN_USER=admin",
		"FILEBROWSER_ADMIN_PASS=''",
		"",
	}, "\n")

	return os.WriteFile(path, []byte(content), 0644)
}

func BootstrapAdminPassword(envPath string) {
	adminPass := strings.TrimSpace(os.Getenv("FILEBROWSER_ADMIN_PASS"))
	if adminPass != "" {
		return
	}

	url := os.Getenv("FILEBROWSER_URL")
	if strings.TrimSpace(url) == "" {
		url = defaultFileBrowserURL
	}

	if !isFileBrowserRunning(url) {
		log.Printf("FileBrowser not reachable at %s; skipping admin password bootstrap", url)
		return
	}

	if _, err := exec.LookPath("filebrowser"); err != nil {
		log.Printf("FileBrowser binary not found in PATH; skipping admin password bootstrap: %v", err)
		return
	}

	newPassword, err := randomStrongHex(32)
	if err != nil {
		log.Printf("Failed generating FileBrowser admin password: %v", err)
		return
	}

	pm2Name, pm2Managed, pm2Online := detectPM2FileBrowserProcess()
	if pm2Managed && pm2Online {
		if err := runPM2Command("stop", pm2Name); err != nil {
			log.Printf("Failed to stop PM2 FileBrowser process %q: %v", pm2Name, err)
		}
		defer func() {
			if err := runPM2Command("start", pm2Name); err != nil {
				log.Printf("Failed to restart PM2 FileBrowser process %q: %v", pm2Name, err)
			}
		}()
	}

	if err := runFileBrowserUpdatePassword(newPassword); err != nil {
		log.Printf("Failed to update FileBrowser admin password: %v", err)
		return
	}

	if err := upsertEnvValue(envPath, "FILEBROWSER_ADMIN_PASS", newPassword); err != nil {
		log.Printf("Failed to persist generated FileBrowser admin password: %v", err)
		return
	}

	_ = os.Setenv("FILEBROWSER_ADMIN_PASS", newPassword)
	log.Printf("FileBrowser admin password was generated and persisted")
}

func Probe(url string) Health {
	if strings.TrimSpace(url) == "" {
		url = defaultFileBrowserURL
	}

	health := Health{Running: isFileBrowserRunning(url), URL: url}
	if !health.Running {
		return health
	}

	health.Version = detectFileBrowserVersion()
	return health
}

func detectPM2FileBrowserProcess() (name string, managed bool, online bool) {
	if _, err := exec.LookPath("pm2"); err != nil {
		return "", false, false
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	out, err := exec.CommandContext(ctx, "pm2", "jlist").CombinedOutput()
	if err != nil {
		return "", false, false
	}

	var processes []pm2Process
	if err := json.Unmarshal(out, &processes); err != nil {
		return "", false, false
	}

	for _, proc := range processes {
		if looksLikeFileBrowser(proc) {
			return proc.Name, true, strings.EqualFold(proc.PM2Env.Status, "online")
		}
	}

	return "", false, false
}

func looksLikeFileBrowser(proc pm2Process) bool {
	joined := strings.ToLower(strings.Join(proc.PM2Env.Args, " "))
	execPath := strings.ToLower(proc.PM2Env.PmExecPath)
	name := strings.ToLower(proc.Name)

	if strings.Contains(name, "filebrowser") {
		return true
	}
	if strings.Contains(execPath, "filebrowser") {
		return true
	}
	return strings.Contains(joined, "filebrowser")
}

func runPM2Command(action, processName string) error {
	if strings.TrimSpace(processName) == "" {
		return errors.New("empty PM2 process name")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	output, err := exec.CommandContext(ctx, "pm2", action, processName).CombinedOutput()
	if err != nil {
		return fmt.Errorf("pm2 %s %s failed: %v: %s", action, processName, err, strings.TrimSpace(string(output)))
	}
	return nil
}

func runFileBrowserUpdatePassword(password string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	output, err := exec.CommandContext(ctx, "filebrowser", "users", "update", "admin", "-p", password).CombinedOutput()
	if err != nil {
		return fmt.Errorf("filebrowser users update admin failed: %v: %s", err, strings.TrimSpace(string(output)))
	}

	return nil
}

func upsertEnvValue(path, key, value string) error {
	content, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	linePrefix := key + "="
	lines := strings.Split(string(content), "\n")
	replaced := false

	for i := range lines {
		if strings.HasPrefix(strings.TrimSpace(lines[i]), linePrefix) {
			lines[i] = linePrefix + value
			replaced = true
			break
		}
	}

	if !replaced {
		lines = append(lines, linePrefix+value)
	}

	updated := strings.Join(lines, "\n")
	if !strings.HasSuffix(updated, "\n") {
		updated += "\n"
	}

	return os.WriteFile(path, []byte(updated), 0644)
}

func isFileBrowserRunning(baseURL string) bool {
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if baseURL == "" {
		return false
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/health", nil)
	if err != nil {
		return false
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	return resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices
}

func detectFileBrowserVersion() string {
	if _, err := exec.LookPath("filebrowser"); err != nil {
		return ""
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	out, err := exec.CommandContext(ctx, "filebrowser", "version").CombinedOutput()
	if err != nil {
		return ""
	}

	return strings.TrimSpace(string(out))
}

func randomStrongHex(bytesLen int) (string, error) {
	if bytesLen < 16 {
		bytesLen = 16
	}

	b := make([]byte, bytesLen)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
