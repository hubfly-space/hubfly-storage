package main

import (
	"flag"
	"fmt"
	"hubfly-storage/volume"
	"log"
	"net/http"
	"os"

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

	fileBrowserBinaryPath := flag.String("filebrowser-binary", "", "optional path to the FileBrowser binary")
	flag.Parse()

	envPath := ".env"
	if err := filebrowser.EnsureEnvFile(envPath); err != nil {
		log.Printf("Failed ensuring .env file: %v", err)
	}

	err := godotenv.Load(envPath)
	if err != nil {
		log.Println("No .env file found")
	}

	resolvedFileBrowserBinaryPath := filebrowser.ResolveBinaryPath(*fileBrowserBinaryPath)
	if resolvedFileBrowserBinaryPath == "" {
		log.Printf("FileBrowser binary unavailable; checked optional path and default fallback")
	} else {
		log.Printf("Using FileBrowser binary at %s", resolvedFileBrowserBinaryPath)
	}

	go filebrowser.BootstrapAdminPassword(envPath, *fileBrowserBinaryPath)

	baseDir := "./docker/volumes"
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		log.Fatalf("Failed to create base directory: %v", err)
	}
	if err := volume.RestoreExistingVolumes(baseDir); err != nil {
		log.Printf("Volume restore completed with warnings: %v", err)
	}

	http.HandleFunc("/create-volume", handlers.CreateVolumeHandler(baseDir))
	http.HandleFunc("/delete-volume", handlers.DeleteVolumeHandler(baseDir))
	http.HandleFunc("/resize-volume", handlers.ResizeVolumeHandler(baseDir))
	http.HandleFunc("/health", handlers.HealthCheckHandler(version, func() handlers.FileBrowserHealth {
		fbHealth := filebrowser.Probe(os.Getenv("FILEBROWSER_URL"), *fileBrowserBinaryPath)
		return handlers.FileBrowserHealth{
			Running: fbHealth.Running,
			Version: fbHealth.Version,
			URL:     fbHealth.URL,
		}
	}))
	http.HandleFunc("/volume-stats", handlers.GetVolumeStatsHandler(baseDir))
	http.HandleFunc("/dev/volumes", handlers.GetVolumesHandler(baseDir))
	http.HandleFunc("/url-volume/create", handlers.URLVolumeCreateHandler(baseDir, resolvedFileBrowserBinaryPath))

	log.Println("🚀 Server running on port 10007...")
	if err := http.ListenAndServe(":10007", nil); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}
