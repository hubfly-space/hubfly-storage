package bootpatch

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	restoreUnitName = "hubfly-storage-volume-restore.service"
	guardUnitName   = "hubfly-storage-container-guard.service"
	dockerDropInDir = "/etc/systemd/system/docker.service.d"
	dockerDropIn    = "10-hubfly-storage-restore.conf"
)

type InstallOptions struct {
	BinaryPath string
	BaseDir    string
	StateDir   string
	EnvPath    string
}

func EnsureInstalled(opts InstallOptions) error {
	if os.Geteuid() != 0 {
		return nil
	}

	if _, err := exec.LookPath("systemctl"); err != nil {
		return nil
	}

	if _, err := os.Stat("/run/systemd/system"); err != nil {
		return nil
	}

	binaryPath, err := filepath.Abs(strings.TrimSpace(opts.BinaryPath))
	if err != nil {
		return fmt.Errorf("resolve binary path: %v", err)
	}
	baseDir, err := filepath.Abs(strings.TrimSpace(opts.BaseDir))
	if err != nil {
		return fmt.Errorf("resolve base dir: %v", err)
	}
	stateDir, err := filepath.Abs(strings.TrimSpace(opts.StateDir))
	if err != nil {
		return fmt.Errorf("resolve state dir: %v", err)
	}
	envPath, err := filepath.Abs(strings.TrimSpace(opts.EnvPath))
	if err != nil {
		return fmt.Errorf("resolve env path: %v", err)
	}

	restoreUnitPath := filepath.Join("/etc/systemd/system", restoreUnitName)
	guardUnitPath := filepath.Join("/etc/systemd/system", guardUnitName)
	dropInPath := filepath.Join(dockerDropInDir, dockerDropIn)

	restoreUnitContent := buildRestoreUnit(binaryPath, baseDir, stateDir, envPath)
	guardUnitContent := buildGuardUnit(binaryPath, stateDir)
	dropInContent := buildDockerDropIn()

	changed := false

	if err := os.MkdirAll(dockerDropInDir, 0755); err != nil {
		return fmt.Errorf("create docker drop-in dir: %v", err)
	}

	wrote, err := writeIfChanged(restoreUnitPath, restoreUnitContent)
	if err != nil {
		return err
	}
	changed = changed || wrote

	wrote, err = writeIfChanged(guardUnitPath, guardUnitContent)
	if err != nil {
		return err
	}
	changed = changed || wrote

	wrote, err = writeIfChanged(dropInPath, dropInContent)
	if err != nil {
		return err
	}
	changed = changed || wrote

	if !changed {
		return nil
	}

	if output, err := exec.Command("systemctl", "daemon-reload").CombinedOutput(); err != nil {
		return fmt.Errorf("systemctl daemon-reload failed: %v: %s", err, strings.TrimSpace(string(output)))
	}

	return nil
}

func writeIfChanged(path, content string) (bool, error) {
	current, err := os.ReadFile(path)
	if err == nil && bytes.Equal(current, []byte(content)) {
		return false, nil
	}
	if err != nil && !os.IsNotExist(err) {
		return false, fmt.Errorf("read %s: %v", path, err)
	}

	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		return false, fmt.Errorf("write %s: %v", path, err)
	}
	return true, nil
}

func buildRestoreUnit(binaryPath, baseDir, stateDir, envPath string) string {
	return strings.Join([]string{
		"[Unit]",
		"Description=Restore Hubfly storage volumes before Docker starts workloads",
		"After=local-fs.target",
		"Before=docker.service",
		"",
		"[Service]",
		"Type=oneshot",
		"User=root",
		"Group=root",
		"TimeoutStartSec=0",
		"ExecStart=" + joinExec(binaryPath, "restore-volumes", "--base-dir="+baseDir, "--state-dir="+stateDir, "--env-file="+envPath),
		"",
	}, "\n")
}

func buildGuardUnit(binaryPath, stateDir string) string {
	return strings.Join([]string{
		"[Unit]",
		"Description=Disable Docker auto-start for containers using failed Hubfly volumes",
		"After=docker.service " + restoreUnitName,
		"Requires=docker.service",
		"",
		"[Service]",
		"Type=oneshot",
		"User=root",
		"Group=root",
		"TimeoutStartSec=0",
		"ExecStart=" + joinExec(binaryPath, "reconcile-containers", "--state-dir="+stateDir),
		"",
	}, "\n")
}

func buildDockerDropIn() string {
	return strings.Join([]string{
		"[Unit]",
		"Wants=" + restoreUnitName + " " + guardUnitName,
		"After=" + restoreUnitName,
		"",
	}, "\n")
}

func joinExec(args ...string) string {
	quoted := make([]string, 0, len(args))
	for _, arg := range args {
		quoted = append(quoted, strconv.Quote(arg))
	}
	return strings.Join(quoted, " ")
}
