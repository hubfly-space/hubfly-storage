package main

import (
	"flag"
	"fmt"
	"hubfly-storage/bootpatch"
	"hubfly-storage/volume"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"hubfly-storage/filebrowser"
	"hubfly-storage/handlers"

	"github.com/joho/godotenv"
)

var version = "dev"

func main() {
	if len(os.Args) > 1 && os.Args[1] == "version" {
		fmt.Println(version)
		return
	}

	switch {
	case len(os.Args) > 1 && os.Args[1] == "restore-volumes":
		runRestoreVolumes()
		return
	case len(os.Args) > 1 && os.Args[1] == "reconcile-containers":
		runReconcileContainers()
		return
	case len(os.Args) > 1 && os.Args[1] == "install-boot-patch":
		runInstallBootPatch()
		return
	}

	fileBrowserBinaryPath := flag.String("filebrowser-binary", "", "optional path to the FileBrowser binary")
	flag.Parse()

	envPath := ".env"
	absEnvPath, err := filepath.Abs(envPath)
	if err != nil {
		log.Fatalf("Failed to resolve env path: %v", err)
	}
	if err := filebrowser.EnsureEnvFile(absEnvPath); err != nil {
		log.Printf("Failed ensuring .env file: %v", err)
	}

	if err := loadEnvFile(absEnvPath); err != nil {
		log.Println("No .env file found")
	}

	resolvedFileBrowserBinaryPath := filebrowser.ResolveBinaryPath(*fileBrowserBinaryPath)
	if resolvedFileBrowserBinaryPath == "" {
		log.Printf("FileBrowser binary unavailable; checked optional path and default fallback")
	} else {
		log.Printf("Using FileBrowser binary at %s", resolvedFileBrowserBinaryPath)
	}

	go filebrowser.BootstrapAdminPassword(absEnvPath, *fileBrowserBinaryPath)

	baseDir := "./docker/volumes"
	absBaseDir, err := filepath.Abs(baseDir)
	if err != nil {
		log.Fatalf("Failed to resolve base directory: %v", err)
	}
	if err := os.MkdirAll(absBaseDir, 0755); err != nil {
		log.Fatalf("Failed to create base directory: %v", err)
	}

	executablePath, err := os.Executable()
	if err != nil {
		log.Printf("Failed to resolve executable path for boot patch: %v", err)
	} else {
		if err := bootpatch.EnsureInstalled(bootpatch.InstallOptions{
			BinaryPath: executablePath,
			BaseDir:    absBaseDir,
			StateDir:   volume.DefaultStateDir,
			EnvPath:    absEnvPath,
		}); err != nil {
			log.Printf("Boot patch install/update warning: %v", err)
		}
	}

	if volume.DockerDaemonRunning() {
		log.Printf("Docker daemon is active; skipping automatic volume restore in API runtime")
	} else {
		if err := volume.RestoreExistingVolumes(absBaseDir, volume.DefaultStateDir); err != nil {
			log.Printf("Volume restore completed with warnings: %v", err)
		}
	}

	http.HandleFunc("/create-volume", handlers.CreateVolumeHandler(absBaseDir))
	http.HandleFunc("/delete-volume", handlers.DeleteVolumeHandler(absBaseDir))
	http.HandleFunc("/resize-volume", handlers.ResizeVolumeHandler(absBaseDir))
	http.HandleFunc("/health", handlers.HealthCheckHandler(version, func() handlers.FileBrowserHealth {
		fbHealth := filebrowser.Probe(os.Getenv("FILEBROWSER_URL"), *fileBrowserBinaryPath)
		return handlers.FileBrowserHealth{
			Running: fbHealth.Running,
			Version: fbHealth.Version,
			URL:     fbHealth.URL,
		}
	}))
	http.HandleFunc("/volume-stats", handlers.GetVolumeStatsHandler(absBaseDir))
	http.HandleFunc("/dev/volumes", handlers.GetVolumesHandler(absBaseDir))
	http.HandleFunc("/url-volume/create", handlers.URLVolumeCreateHandler(absBaseDir, resolvedFileBrowserBinaryPath))

	log.Println("🚀 Server running on port 10007...")
	if err := http.ListenAndServe(":10007", nil); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}

func runRestoreVolumes() {
	fs := flag.NewFlagSet("restore-volumes", flag.ExitOnError)
	baseDir := fs.String("base-dir", "./docker/volumes", "base directory for Hubfly volumes")
	stateDir := fs.String("state-dir", volume.DefaultStateDir, "directory for volume restore state")
	envFile := fs.String("env-file", "", "optional env file path")
	_ = fs.Parse(os.Args[2:])

	if strings.TrimSpace(*envFile) != "" {
		if err := loadEnvFile(*envFile); err != nil {
			log.Printf("warning: failed to load env file %s: %v", *envFile, err)
		}
	}

	if err := volume.RestoreExistingVolumes(*baseDir, *stateDir); err != nil {
		log.Printf("Volume restore completed with warnings: %v", err)
	}
}

func runReconcileContainers() {
	fs := flag.NewFlagSet("reconcile-containers", flag.ExitOnError)
	stateDir := fs.String("state-dir", volume.DefaultStateDir, "directory for volume restore state")
	_ = fs.Parse(os.Args[2:])

	if err := volume.ReconcileContainerAutostart(*stateDir); err != nil {
		log.Printf("Container reconcile completed with warnings: %v", err)
	}
}

func runInstallBootPatch() {
	fs := flag.NewFlagSet("install-boot-patch", flag.ExitOnError)
	baseDir := fs.String("base-dir", "./docker/volumes", "base directory for Hubfly volumes")
	stateDir := fs.String("state-dir", volume.DefaultStateDir, "directory for volume restore state")
	envFile := fs.String("env-file", ".env", "env file path for restore command")
	_ = fs.Parse(os.Args[2:])

	binaryPath, err := os.Executable()
	if err != nil {
		log.Fatalf("Failed to resolve executable path: %v", err)
	}

	if err := bootpatch.EnsureInstalled(bootpatch.InstallOptions{
		BinaryPath: binaryPath,
		BaseDir:    *baseDir,
		StateDir:   *stateDir,
		EnvPath:    *envFile,
	}); err != nil {
		log.Fatalf("Failed to install boot patch: %v", err)
	}
}

func loadEnvFile(path string) error {
	if strings.TrimSpace(path) == "" {
		return nil
	}
	return godotenv.Load(path)
}
