package volume

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

const DefaultStateDir = "/var/lib/hubfly-storage/state"

type VolumeRestoreStatus struct {
	Name    string `json:"name"`
	Ready   bool   `json:"ready"`
	Source  string `json:"source,omitempty"`
	Message string `json:"message,omitempty"`
}

type RestoreReport struct {
	UpdatedAt time.Time             `json:"updated_at"`
	Volumes   []VolumeRestoreStatus `json:"volumes"`
}

type disabledContainerRestartPolicy struct {
	ContainerID       string `json:"container_id"`
	ContainerName     string `json:"container_name"`
	RestartPolicyName string `json:"restart_policy_name"`
	MaximumRetryCount int    `json:"maximum_retry_count"`
}

type dockerInspect struct {
	ID    string `json:"Id"`
	Name  string `json:"Name"`
	State struct {
		Running bool `json:"Running"`
	} `json:"State"`
	HostConfig struct {
		RestartPolicy struct {
			Name              string `json:"Name"`
			MaximumRetryCount int    `json:"MaximumRetryCount"`
		} `json:"RestartPolicy"`
	} `json:"HostConfig"`
	Mounts []struct {
		Type   string `json:"Type"`
		Name   string `json:"Name"`
		Source string `json:"Source"`
	} `json:"Mounts"`
}

func RestoreExistingVolumes(baseDir, stateDir string) error {
	report, err := restoreExistingVolumesWithReport(baseDir)
	if persistErr := SaveRestoreReport(stateDir, report); persistErr != nil {
		if err == nil {
			err = persistErr
		} else {
			err = fmt.Errorf("%v; additionally failed to persist restore report: %v", err, persistErr)
		}
	}
	return err
}

func restoreExistingVolumesWithReport(baseDir string) (*RestoreReport, error) {
	report := &RestoreReport{
		UpdatedAt: time.Now().UTC(),
		Volumes:   []VolumeRestoreStatus{},
	}

	files, err := os.ReadDir(baseDir)
	if err != nil {
		return report, fmt.Errorf("failed to read base directory: %v", err)
	}

	var restoreErrors []string
	for _, file := range files {
		if !file.IsDir() {
			continue
		}

		name := file.Name()
		source, volumeErr := ensureHubflyVolumeMount(name, baseDir)
		status := VolumeRestoreStatus{Name: name}
		if volumeErr != nil {
			status.Message = volumeErr.Error()
			report.Volumes = append(report.Volumes, status)
			restoreErrors = append(restoreErrors, fmt.Sprintf("%s: %v", name, volumeErr))
			continue
		}

		mounted, mountSource, inspectErr := isVolumeMounted(filepath.Join(baseDir, name, "_data"))
		if inspectErr != nil {
			status.Message = inspectErr.Error()
			report.Volumes = append(report.Volumes, status)
			restoreErrors = append(restoreErrors, fmt.Sprintf("%s: %v", name, inspectErr))
			continue
		}

		if mounted && isExpectedVolumeMountSource(mountSource) {
			status.Ready = true
			status.Source = mountSource
		} else {
			status.Source = source
			status.Message = fmt.Sprintf("volume mounted from unexpected source %q after restore", mountSource)
			restoreErrors = append(restoreErrors, fmt.Sprintf("%s: %s", name, status.Message))
		}

		report.Volumes = append(report.Volumes, status)
	}

	if len(restoreErrors) > 0 {
		return report, fmt.Errorf("volume restore warnings: %s", strings.Join(restoreErrors, "; "))
	}

	return report, nil
}

func SaveRestoreReport(stateDir string, report *RestoreReport) error {
	if report == nil {
		report = &RestoreReport{UpdatedAt: time.Now().UTC(), Volumes: []VolumeRestoreStatus{}}
	}

	if err := os.MkdirAll(stateDir, 0755); err != nil {
		return fmt.Errorf("failed to create state dir %s: %v", stateDir, err)
	}

	payload, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to encode restore report: %v", err)
	}

	if err := os.WriteFile(filepath.Join(stateDir, "restore-report.json"), payload, 0644); err != nil {
		return fmt.Errorf("failed to write restore report: %v", err)
	}

	return nil
}

func LoadRestoreReport(stateDir string) (*RestoreReport, error) {
	content, err := os.ReadFile(filepath.Join(stateDir, "restore-report.json"))
	if err != nil {
		if os.IsNotExist(err) {
			return &RestoreReport{Volumes: []VolumeRestoreStatus{}}, nil
		}
		return nil, fmt.Errorf("failed to read restore report: %v", err)
	}

	var report RestoreReport
	if err := json.Unmarshal(content, &report); err != nil {
		return nil, fmt.Errorf("failed to decode restore report: %v", err)
	}
	if report.Volumes == nil {
		report.Volumes = []VolumeRestoreStatus{}
	}

	return &report, nil
}

func ReconcileContainerAutostart(stateDir string) error {
	if !isDockerDaemonRunning() {
		return nil
	}

	report, err := LoadRestoreReport(stateDir)
	if err != nil {
		return err
	}

	failedVolumes := make(map[string]VolumeRestoreStatus)
	for _, status := range report.Volumes {
		if !status.Ready {
			failedVolumes[status.Name] = status
		}
	}

	containers, err := inspectAllContainers()
	if err != nil {
		return err
	}

	policies, err := loadDisabledRestartPolicies(stateDir)
	if err != nil {
		return err
	}

	for _, container := range containers {
		usesFailed := false
		for _, mount := range container.Mounts {
			if mount.Type != "volume" {
				continue
			}
			if _, exists := failedVolumes[mount.Name]; exists {
				usesFailed = true
				break
			}
		}

		name := strings.TrimPrefix(container.Name, "/")
		if usesFailed {
			if container.HostConfig.RestartPolicy.Name != "" && container.HostConfig.RestartPolicy.Name != "no" {
				policies[container.ID] = disabledContainerRestartPolicy{
					ContainerID:       container.ID,
					ContainerName:     name,
					RestartPolicyName: container.HostConfig.RestartPolicy.Name,
					MaximumRetryCount: container.HostConfig.RestartPolicy.MaximumRetryCount,
				}

				restartValue := "no"
				log.Printf("Disabling auto-start for container %s (%s) because it uses failed Hubfly volumes", name, container.ID)
				if err := runCommand("docker", "update", "--restart="+restartValue, container.ID); err != nil {
					log.Printf("warning: failed to disable restart policy for %s: %v", name, err)
				}
			}

			if container.State.Running {
				log.Printf("Stopping container %s (%s) because it uses failed Hubfly volumes", name, container.ID)
				if err := runCommand("docker", "stop", "-t", "20", container.ID); err != nil {
					log.Printf("warning: failed to stop container %s: %v", name, err)
				}
			}
			continue
		}

		if previous, exists := policies[container.ID]; exists {
			restartValue := previous.RestartPolicyName
			if restartValue == "" {
				restartValue = "no"
			}
			if restartValue == "on-failure" && previous.MaximumRetryCount > 0 {
				restartValue = fmt.Sprintf("%s:%d", restartValue, previous.MaximumRetryCount)
			}

			log.Printf("Restoring restart policy %s for container %s (%s)", restartValue, name, container.ID)
			if err := runCommand("docker", "update", "--restart="+restartValue, container.ID); err != nil {
				log.Printf("warning: failed to restore restart policy for %s: %v", name, err)
				continue
			}

			delete(policies, container.ID)
		}
	}

	return saveDisabledRestartPolicies(stateDir, policies)
}

func inspectAllContainers() ([]dockerInspect, error) {
	output, err := runCommandWithOutput("docker", "ps", "-aq")
	if err != nil {
		return nil, fmt.Errorf("failed to list docker containers: %v", err)
	}

	ids := strings.Fields(output)
	if len(ids) == 0 {
		return []dockerInspect{}, nil
	}

	args := append([]string{"inspect"}, ids...)
	output, err = runCommandWithOutput("docker", args...)
	if err != nil {
		return nil, fmt.Errorf("failed to inspect docker containers: %v", err)
	}

	var containers []dockerInspect
	if err := json.Unmarshal([]byte(output), &containers); err != nil {
		return nil, fmt.Errorf("failed to decode docker inspect output: %v", err)
	}

	return containers, nil
}

func disabledRestartPoliciesPath(stateDir string) string {
	return filepath.Join(stateDir, "disabled-restart-policies.json")
}

func loadDisabledRestartPolicies(stateDir string) (map[string]disabledContainerRestartPolicy, error) {
	content, err := os.ReadFile(disabledRestartPoliciesPath(stateDir))
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]disabledContainerRestartPolicy{}, nil
		}
		return nil, fmt.Errorf("failed to read disabled restart policy state: %v", err)
	}

	var policies map[string]disabledContainerRestartPolicy
	if err := json.Unmarshal(content, &policies); err != nil {
		return nil, fmt.Errorf("failed to decode disabled restart policy state: %v", err)
	}
	if policies == nil {
		policies = map[string]disabledContainerRestartPolicy{}
	}

	return policies, nil
}

func saveDisabledRestartPolicies(stateDir string, policies map[string]disabledContainerRestartPolicy) error {
	if err := os.MkdirAll(stateDir, 0755); err != nil {
		return fmt.Errorf("failed to create state dir %s: %v", stateDir, err)
	}

	payload, err := json.MarshalIndent(policies, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to encode disabled restart policy state: %v", err)
	}

	if err := os.WriteFile(disabledRestartPoliciesPath(stateDir), payload, 0644); err != nil {
		return fmt.Errorf("failed to write disabled restart policy state: %v", err)
	}

	return nil
}

func isDockerDaemonRunning() bool {
	if _, err := exec.LookPath("systemctl"); err == nil {
		if err := exec.Command("systemctl", "is-active", "--quiet", "docker").Run(); err == nil {
			return true
		}
	}

	if _, err := exec.LookPath("docker"); err != nil {
		return false
	}

	if err := exec.Command("docker", "info").Run(); err == nil {
		return true
	}

	return false
}

func DockerDaemonRunning() bool {
	return isDockerDaemonRunning()
}
