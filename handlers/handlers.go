package handlers

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hubfly-storage/volume"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
)

// Structs for File Browser API
type LoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type LoginResponse struct {
	Token string `json:"token"`
}

type CreateUserRequest struct {
	What  string   `json:"what"`
	Which []string `json:"which"`
	Data  UserData `json:"data"`
}

type UserData struct {
	Scope          string      `json:"scope"`
	Locale         string      `json:"locale"`
	ViewMode       string      `json:"viewMode"`
	SingleClick    bool        `json:"singleClick"`
	Sorting        Sorting     `json:"sorting"`
	Perm           Permissions `json:"perm"`
	Commands       []string    `json:"commands"`
	HideDotfiles   bool        `json:"hideDotfiles"`
	DateFormat     bool        `json:"dateFormat"`
	AceEditorTheme string      `json:"aceEditorTheme"`
	Username       string      `json:"username"`
	Password       string      `json:"password"`
	Rules          []Rule      `json:"rules"`
	LockPassword   bool        `json:"lockPassword"`
	ID             int         `json:"id"`
}

type Sorting struct {
	By  string `json:"by"`
	Asc bool   `json:"asc"`
}

type Permissions struct {
	Admin    bool `json:"admin"`
	Execute  bool `json:"execute"`
	Create   bool `json:"create"`
	Rename   bool `json:"rename"`
	Modify   bool `json:"modify"`
	Delete   bool `json:"delete"`
	Share    bool `json:"share"`
	Download bool `json:"download"`
}

type Rule struct{}

type GetTokenResponse struct {
	URL string `json:"url"`
}

type URLVolumeCreateRequest struct {
	Name string `json:"name"`
}

type DockerVolumePayload struct {
	Name       string            `json:"Name"`
	Driver     string            `json:"Driver"`
	DriverOpts map[string]string `json:"DriverOpts"`
	Labels     map[string]string `json:"Labels"`
}

type FileBrowserHealth struct {
	Running bool   `json:"running"`
	Version string `json:"version,omitempty"`
	URL     string `json:"url,omitempty"`
}

func handleError(w http.ResponseWriter, msg string, statusCode int) {
	log.Println("❌ " + msg)
	http.Error(w, msg, statusCode)
}

func CreateVolumeHandler(baseDir string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var payload DockerVolumePayload
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			handleError(w, fmt.Sprintf("Invalid JSON payload: %v", err), http.StatusBadRequest)
			return
		}
		defer r.Body.Close()

		log.Printf("Received request to create volume: %s", payload.Name)

		size := payload.DriverOpts["size"]
		if size == "" {
			size = "1G"
		}

		enableEncryption, err := parseOptionalBool(payload.DriverOpts["encryption"])
		if err != nil {
			handleError(w, fmt.Sprintf("Invalid encryption value: %v", err), http.StatusBadRequest)
			return
		}

		optimization := payload.DriverOpts["optimization"]
		if optimization == "" {
			optimization = "standard"
		}

		config := volume.VolumeConfig{
			Size:             size,
			EnableEncryption: enableEncryption,
			EncryptionKey:    payload.DriverOpts["encryption_key"],
			Optimization:     optimization,
			Labels:           payload.Labels,
		}

		volName, err := volume.CreateVolume(payload.Name, baseDir, config)
		if err != nil {
			handleError(w, fmt.Sprintf("Failed to create volume: %v", err), http.StatusInternalServerError)
			return
		}

		log.Printf("Volume %s created successfully!", volName)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{
			"status": "success",
			"name":   volName,
		})
	}
}

func parseOptionalBool(raw string) (bool, error) {
	if strings.TrimSpace(raw) == "" {
		return false, nil
	}

	parsed, err := strconv.ParseBool(strings.TrimSpace(raw))
	if err != nil {
		return false, fmt.Errorf("expected one of true/false/1/0")
	}

	return parsed, nil
}

func DeleteVolumeHandler(baseDir string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var payload DockerVolumePayload
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			handleError(w, fmt.Sprintf("Invalid JSON payload: %v", err), http.StatusBadRequest)
			return
		}
		defer r.Body.Close()

		log.Printf("Received request to delete volume: %s", payload.Name)

		if err := volume.DeleteVolume(payload.Name, baseDir); err != nil {
			handleError(w, fmt.Sprintf("Failed to delete volume: %v", err), http.StatusInternalServerError)
			return
		}

		log.Printf("Volume %s deleted successfully!", payload.Name)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{
			"status": "success",
			"name":   payload.Name,
		})
	}
}

func HealthCheckHandler(storageVersion string, getFileBrowserHealth func() FileBrowserHealth) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		response := map[string]interface{}{
			"status":  "ok",
			"service": "hubfly-storage",
			"version": storageVersion,
		}

		if getFileBrowserHealth != nil {
			response["filebrowser"] = getFileBrowserHealth()
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(response)
	}
}

func GetVolumeStatsHandler(baseDir string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var payload DockerVolumePayload
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			handleError(w, fmt.Sprintf("Invalid JSON payload: %v", err), http.StatusBadRequest)
			return
		}
		defer r.Body.Close()

		log.Printf("Received request for volume stats: %s", payload.Name)

		stats, err := volume.GetVolumeStats(payload.Name, baseDir)
		if err != nil {
			handleError(w, fmt.Sprintf("Failed to get volume stats: %v", err), http.StatusInternalServerError)
			return
		}

		log.Printf("Volume stats for %s retrieved successfully!", payload.Name)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(stats)
	}
}

func GetVolumesHandler(baseDir string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		log.Printf("Received request to get all volumes")

		volumes, err := volume.GetAllVolumes(baseDir)
		if err != nil {
			handleError(w, fmt.Sprintf("Failed to get volumes: %v", err), http.StatusInternalServerError)
			return
		}

		log.Printf("Volumes retrieved successfully!")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(volumes)
	}
}

func URLVolumeCreateHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		handleError(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	var req URLVolumeCreateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		handleError(w, fmt.Sprintf("Invalid JSON payload: %v", err), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	filebrowserURL := os.Getenv("FILEBROWSER_URL")
	adminUser := os.Getenv("FILEBROWSER_ADMIN_USER")
	adminPass := os.Getenv("FILEBROWSER_ADMIN_PASS")

	// Step 1: Admin Login
	adminToken, err := loginFileBrowser(filebrowserURL, adminUser, adminPass)
	if err != nil {
		handleError(w, fmt.Sprintf("Failed to login as admin: %v", err), http.StatusInternalServerError)
		return
	}

	// Step 2: Create a temporary user
	tempUser := "tempuser_" + randomHex(8)
	tempPass := randomHex(16)
	err = createTempUser(filebrowserURL, adminToken, req.Name, tempUser, tempPass)
	if err != nil {
		handleError(w, fmt.Sprintf("Failed to create temp user: %v", err), http.StatusInternalServerError)
		return
	}

	// Step 3: Login as the temporary user
	userToken, err := loginFileBrowser(filebrowserURL, tempUser, tempPass)
	if err != nil {
		handleError(w, fmt.Sprintf("Failed to login as temp user: %v", err), http.StatusInternalServerError)
		return
	}

	// Step 4: Get login token URL
	tokenURL, err := getLoginTokenURL(filebrowserURL, userToken)
	if err != nil {
		handleError(w, fmt.Sprintf("Failed to get login token URL: %v", err), http.StatusInternalServerError)
		return
	}

	fullURL := filebrowserURL + tokenURL

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"url": fullURL})
}

func loginFileBrowser(baseURL, username, password string) (string, error) {
	loginURL := baseURL + "/api/login"
	loginReq := LoginRequest{Username: username, Password: password}
	reqBody, err := json.Marshal(loginReq)
	if err != nil {
		return "", fmt.Errorf("failed to marshal login request: %v", err)
	}

	resp, err := http.Post(loginURL, "application/json", bytes.NewBuffer(reqBody))
	if err != nil {
		return "", fmt.Errorf("login request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := ioutil.ReadAll(resp.Body)
		return "", fmt.Errorf("login failed with status %d: %s", resp.StatusCode, string(bodyBytes))
	}
	// Read the response body
	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read login response body: %v", err)
	}

	var loginResp LoginResponse
	// The token is directly in the body, not in a JSON object
	loginResp.Token = string(bodyBytes)

	return loginResp.Token, nil
}

func createTempUser(baseURL, adminToken, volumeName, username, password string) error {
	usersURL := baseURL + "/api/users"
	createUserReq := CreateUserRequest{
		What:  "user",
		Which: []string{},
		Data: UserData{
			Scope:       "/volumes/" + volumeName + "/_data",
			Locale:      "en",
			ViewMode:    "mosaic",
			SingleClick: false,
			Sorting:     Sorting{By: "", Asc: false},
			Perm: Permissions{
				Admin:    false,
				Execute:  false,
				Create:   true,
				Rename:   true,
				Modify:   true,
				Delete:   true,
				Share:    false,
				Download: true,
			},
			Commands:     []string{},
			HideDotfiles: false,
			DateFormat:   false,
			Username:     username,
			Password:     password,
			Rules:        []Rule{},
			LockPassword: true,
			ID:           0,
		},
	}
	reqBody, err := json.Marshal(createUserReq)
	if err != nil {
		return fmt.Errorf("failed to marshal create user request: %v", err)
	}

	req, err := http.NewRequest("POST", usersURL, bytes.NewBuffer(reqBody))
	if err != nil {
		return fmt.Errorf("failed to create new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Auth", adminToken)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("create user request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		bodyBytes, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf("create user failed with status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	return nil
}

func getLoginTokenURL(baseURL, userToken string) (string, error) {
	tokenURL := baseURL + "/api/login/token"
	req, err := http.NewRequest("POST", tokenURL, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create new request: %v", err)
	}
	req.Header.Set("X-Auth", userToken)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("get login token request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := ioutil.ReadAll(resp.Body)
		return "", fmt.Errorf("get login token failed with status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var tokenResp GetTokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&tokenResp); err != nil {
		return "", fmt.Errorf("failed to decode get token response: %v", err)
	}

	return tokenResp.URL, nil
}

func randomHex(n int) string {
	bytes := make([]byte, n)
	if _, err := rand.Read(bytes); err != nil {
		panic(err)
	}
	return hex.EncodeToString(bytes)
}
