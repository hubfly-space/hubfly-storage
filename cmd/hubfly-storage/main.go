package main

import (
	"fmt"
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

	envPath := ".env"
	if err := filebrowser.EnsureEnvFile(envPath); err != nil {
		log.Printf("Failed ensuring .env file: %v", err)
	}

	err := godotenv.Load(envPath)
	if err != nil {
		log.Println("No .env file found")
	}

	go filebrowser.BootstrapAdminPassword(envPath)

	baseDir := "./docker/volumes"
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		log.Fatalf("Failed to create base directory: %v", err)
	}

	http.HandleFunc("/create-volume", handlers.CreateVolumeHandler(baseDir))
	http.HandleFunc("/delete-volume", handlers.DeleteVolumeHandler(baseDir))
	http.HandleFunc("/health", handlers.HealthCheckHandler())
	http.HandleFunc("/volume-stats", handlers.GetVolumeStatsHandler(baseDir))
	http.HandleFunc("/dev/volumes", handlers.GetVolumesHandler(baseDir))
	http.HandleFunc("/url-volume/create", handlers.URLVolumeCreateHandler)

	log.Println("🚀 Server running on port 8203...")
	if err := http.ListenAndServe(":8203", nil); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}
