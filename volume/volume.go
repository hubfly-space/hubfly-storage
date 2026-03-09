package volume

import (
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

type OptimizationMode string

const (
	OptimizationStandard        OptimizationMode = "standard"
	OptimizationHighPerformance OptimizationMode = "high_performance"
	OptimizationBalanced        OptimizationMode = "balanced"
)

type VolumeConfig struct {
	Size             string
	EnableEncryption bool
	EncryptionKey    string
	Optimization     string
	Labels           map[string]string
}

type VolumeStats struct {
	Name      string `json:"name"`
	Size      string `json:"size"`
	Used      string `json:"used"`
	Available string `json:"available"`
	Usage     string `json:"usage"`
	MountPath string `json:"mount_path"`
}

var sizePattern = regexp.MustCompile(`^([0-9]+(?:\.[0-9]+)?)\s*([a-zA-Z]*)$`)

type ValidationError struct {
	message string
}

func (e *ValidationError) Error() string {
	return e.message
}

func validationErrorf(format string, args ...interface{}) error {
	return &ValidationError{message: fmt.Sprintf(format, args...)}
}

func IsValidationError(err error) bool {
	var validationErr *ValidationError
	return errors.As(err, &validationErr)
}

func runCommand(name string, args ...string) error {
	cmd := exec.Command(name, args...)
	output, err := cmd.CombinedOutput()
	log.Printf("Command: %s %v\nOutput: %s", name, args, output)
	if err != nil {
		return fmt.Errorf("%v: %s", err, output)
	}
	return nil
}

func runCommandWithInput(input, name string, args ...string) error {
	cmd := exec.Command(name, args...)
	cmd.Stdin = strings.NewReader(input)
	output, err := cmd.CombinedOutput()
	log.Printf("Command: %s %v\nOutput: %s", name, args, output)
	if err != nil {
		return fmt.Errorf("%v: %s", err, output)
	}
	return nil
}

func runCommandWithOutput(name string, args ...string) (string, error) {
	cmd := exec.Command(name, args...)
	output, err := cmd.CombinedOutput()
	log.Printf("Command: %s %v\nOutput: %s", name, args, output)
	if err != nil {
		return string(output), fmt.Errorf("%v: %s", err, output)
	}
	return string(output), nil
}

func volumeExists(name string) (bool, error) {
	output, err := runCommandWithOutput("docker", "volume", "ls", "-q", "-f", "name="+name)
	if err != nil {
		return false, fmt.Errorf("failed to check if volume exists: %v", err)
	}
	exists := strings.TrimSpace(output) == name
	return exists, nil
}

func CreateVolume(name, baseDir string, config VolumeConfig) (string, error) {
	exists, err := volumeExists(name)
	if err != nil {
		return "", fmt.Errorf("failed to check for existing volume: %v", err)
	}
	if exists {
		return "", fmt.Errorf("volume '%s' already exists", name)
	}

	normalizedMode, err := normalizeOptimization(config.Optimization)
	if err != nil {
		return "", err
	}

	encryptionKey, err := resolveEncryptionKey(config)
	if err != nil {
		return "", err
	}

	volumePath := filepath.Join(baseDir, name)
	dataPath := filepath.Join(volumePath, "_data")
	absDataPath, err := filepath.Abs(dataPath)
	if err != nil {
		return "", fmt.Errorf("failed to resolve absolute path: %v", err)
	}
	imagePath := filepath.Join(volumePath, "volume.img")

	size := config.Size
	if size == "" {
		size = "1G"
	}

	if err := os.MkdirAll(dataPath, 0755); err != nil {
		return "", fmt.Errorf("failed to create directory: %v", err)
	}

	mounted := false
	encryptedOpened := false
	dockerRegistered := false
	success := false
	defer func() {
		if success {
			return
		}
		if dockerRegistered {
			if err := runCommand("docker", "volume", "rm", name); err != nil {
				log.Printf("rollback warning: failed to remove docker volume %s: %v", name, err)
			}
		}
		if mounted {
			if err := runCommand("sudo", "umount", dataPath); err != nil {
				log.Printf("rollback warning: failed to unmount %s: %v", dataPath, err)
			}
		}
		if encryptedOpened {
			if err := closeEncryptionMapping(name); err != nil {
				log.Printf("rollback warning: failed to close encryption mapping %s: %v", name, err)
			}
		}
		if err := os.RemoveAll(volumePath); err != nil {
			log.Printf("rollback warning: failed to remove volume path %s: %v", volumePath, err)
		}
	}()

	log.Printf("Allocating %s image file at %s", size, imagePath)
	if err := runCommand("sudo", "fallocate", "-l", size, imagePath); err != nil {
		return "", fmt.Errorf("fallocate failed: %v", err)
	}

	mountSource := imagePath
	if config.EnableEncryption {
		mapperName := mapperNameForVolume(name)
		if err := setupEncryptedDevice(imagePath, mapperName, encryptionKey); err != nil {
			return "", err
		}
		mountSource = mapperPath(mapperName)
		encryptedOpened = true
	}

	log.Printf("Formatting %s as ext4", mountSource)
	if err := runCommand("sudo", "mkfs.ext4", mountSource); err != nil {
		return "", fmt.Errorf("mkfs.ext4 failed: %v", err)
	}

	mountOpts := mountOptionsForMode(normalizedMode)
	log.Printf("Mounting volume image at %s with options: %s", dataPath, mountOpts)
	if err := runCommand("sudo", "mount", "-o", mountOpts, mountSource, dataPath); err != nil {
		return "", fmt.Errorf("mount failed: %v", err)
	}
	mounted = true

	lostAndFoundPath := filepath.Join(dataPath, "lost+found")
	log.Printf("Removing lost+found directory: %s", lostAndFoundPath)
	if err := runCommand("sudo", "rm", "-rf", lostAndFoundPath); err != nil {
		log.Printf("warning: failed to remove lost+found: %v", err)
	}

	log.Printf("Setting permissions for data directory: %s to 777", absDataPath)
	if err := runCommand("sudo", "chmod", "-R", "777", absDataPath); err != nil {
		return "", fmt.Errorf("chmod failed: %v", err)
	}

	if sudoUser := os.Getenv("SUDO_USER"); sudoUser != "" {
		log.Printf("Setting ownership for data directory: %s to %s", dataPath, sudoUser)
		if err := runCommand("sudo", "chown", "-R", fmt.Sprintf("%s:%s", sudoUser, sudoUser), dataPath); err != nil {
			return "", fmt.Errorf("chown failed: %v", err)
		}
	}

	log.Printf("Registering docker volume: %s", name)
	dockerArgs := []string{
		"docker", "volume", "create",
		"--name", name,
		"--opt", fmt.Sprintf("device=%s", absDataPath),
		"--opt", "type=none",
		"--opt", "o=bind",
	}

	for key, value := range config.Labels {
		dockerArgs = append(dockerArgs, "--label", fmt.Sprintf("%s=%s", key, value))
	}

	if err := runCommand(dockerArgs[0], dockerArgs[1:]...); err != nil {
		return "", fmt.Errorf("docker volume create failed: %v", err)
	}
	dockerRegistered = true

	success = true
	return name, nil
}

func DeleteVolume(name, baseDir string) error {
	volumePath := filepath.Join(baseDir, name)
	dataPath := filepath.Join(volumePath, "_data")

	log.Printf("Unmounting volume at %s", dataPath)
	if err := runCommand("sudo", "umount", dataPath); err != nil {
		log.Printf("unmount failed (might be acceptable if not mounted): %v", err)
	}

	if err := closeEncryptionMapping(name); err != nil {
		log.Printf("warning: failed to close encryption mapping for %s: %v", name, err)
	}

	log.Printf("Removing docker volume: %s", name)
	if err := runCommand("docker", "volume", "rm", name); err != nil {
		return fmt.Errorf("docker volume rm failed: %v", err)
	}

	log.Printf("Removing volume directory: %s", volumePath)
	if err := os.RemoveAll(volumePath); err != nil {
		return fmt.Errorf("failed to remove volume directory: %v", err)
	}

	return nil
}

func ResizeVolume(name, baseDir, requestedSize string) (int64, int64, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return 0, 0, validationErrorf("volume name is required")
	}

	requestedSize = strings.TrimSpace(requestedSize)
	if requestedSize == "" {
		return 0, 0, validationErrorf("requested size is required")
	}

	requestedBytes, err := parseSizeToBytes(requestedSize)
	if err != nil {
		return 0, 0, validationErrorf("invalid requested size: %v", err)
	}

	volumePath := filepath.Join(baseDir, name)
	dataPath := filepath.Join(volumePath, "_data")
	imagePath := filepath.Join(volumePath, "volume.img")

	info, err := os.Stat(imagePath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, 0, validationErrorf("volume image not found for '%s'", name)
		}
		return 0, 0, fmt.Errorf("failed to inspect volume image: %v", err)
	}

	currentBytes := info.Size()
	if requestedBytes <= currentBytes {
		return 0, 0, validationErrorf("new size must be greater than current size (%d bytes); scaling down is not supported", currentBytes)
	}

	log.Printf("Resizing volume image for %s from %d to %d bytes", name, currentBytes, requestedBytes)
	if err := runCommand("sudo", "fallocate", "-l", strconv.FormatInt(requestedBytes, 10), imagePath); err != nil {
		return 0, 0, fmt.Errorf("fallocate failed: %v", err)
	}

	mapperName := mapperNameForVolume(name)
	mapperDevice := mapperPath(mapperName)
	if _, err := os.Stat(mapperDevice); err == nil {
		log.Printf("Resizing encrypted mapper %s", mapperName)
		if err := runCommand("sudo", "cryptsetup", "resize", mapperName); err != nil {
			return currentBytes, requestedBytes, fmt.Errorf("cryptsetup resize failed: %v", err)
		}
	} else if err != nil && !os.IsNotExist(err) {
		return currentBytes, requestedBytes, fmt.Errorf("failed to inspect encryption mapper: %v", err)
	}

	resizeTarget, err := detectResizeTarget(dataPath, mapperDevice, imagePath)
	if err != nil {
		return currentBytes, requestedBytes, fmt.Errorf("failed to detect resize target: %v", err)
	}

	log.Printf("Growing ext4 filesystem for %s using target %s", name, resizeTarget)
	if err := runCommand("sudo", "resize2fs", resizeTarget); err != nil {
		return currentBytes, requestedBytes, fmt.Errorf("resize2fs failed after image growth; rerun resize once mount state is healthy: %v", err)
	}

	return currentBytes, requestedBytes, nil
}

func setupEncryptedDevice(imagePath, mapperName, key string) error {
	log.Printf("Creating LUKS2 encrypted device for %s", imagePath)
	if err := runCommandWithInput(key+"\n", "sudo", "cryptsetup", "-q", "luksFormat", "--type", "luks2", imagePath, "-"); err != nil {
		return fmt.Errorf("cryptsetup luksFormat failed: %v", err)
	}

	log.Printf("Opening encrypted device mapping %s", mapperName)
	if err := runCommandWithInput(key+"\n", "sudo", "cryptsetup", "open", imagePath, mapperName, "-"); err != nil {
		return fmt.Errorf("cryptsetup open failed: %v", err)
	}

	return nil
}

func closeEncryptionMapping(volumeName string) error {
	mapperName := mapperNameForVolume(volumeName)
	if _, err := os.Stat(mapperPath(mapperName)); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	if err := runCommand("sudo", "cryptsetup", "close", mapperName); err != nil {
		return fmt.Errorf("cryptsetup close failed: %v", err)
	}
	return nil
}

func mapperNameForVolume(volumeName string) string {
	cleaned := strings.ToLower(strings.TrimSpace(volumeName))
	cleaned = strings.ReplaceAll(cleaned, " ", "-")
	cleaned = strings.ReplaceAll(cleaned, "/", "-")
	return "hubfly-" + cleaned
}

func mapperPath(mapperName string) string {
	return filepath.Join("/dev/mapper", mapperName)
}

func resolveEncryptionKey(config VolumeConfig) (string, error) {
	if !config.EnableEncryption {
		return "", nil
	}

	if strings.TrimSpace(config.EncryptionKey) != "" {
		return config.EncryptionKey, nil
	}

	envKey := os.Getenv("VOLUME_ENCRYPTION_KEY")
	if strings.TrimSpace(envKey) != "" {
		return envKey, nil
	}

	return "", fmt.Errorf("encryption requested but no key provided; set DriverOpts.encryption_key or VOLUME_ENCRYPTION_KEY")
}

func normalizeOptimization(raw string) (OptimizationMode, error) {
	modeRaw := strings.ToLower(strings.TrimSpace(raw))
	modeRaw = strings.ReplaceAll(modeRaw, "-", "_")
	modeRaw = strings.ReplaceAll(modeRaw, " ", "_")
	if modeRaw == "high_perfomance" {
		modeRaw = string(OptimizationHighPerformance)
	}
	mode := OptimizationMode(modeRaw)
	if mode == "" {
		return OptimizationStandard, nil
	}

	switch mode {
	case OptimizationStandard, OptimizationHighPerformance, OptimizationBalanced:
		return mode, nil
	default:
		return "", fmt.Errorf("unsupported optimization mode '%s'; expected one of: standard, high_performance, balanced", raw)
	}
}

func mountOptionsForMode(mode OptimizationMode) string {
	switch mode {
	case OptimizationHighPerformance:
		return "noatime,nodiratime,commit=60,data=writeback"
	case OptimizationBalanced:
		return "relatime,commit=30"
	default:
		return "defaults"
	}
}

func detectResizeTarget(dataPath, mapperDevice, imagePath string) (string, error) {
	mountSource, err := mountedSourceForTarget(dataPath)
	if err == nil && strings.TrimSpace(mountSource) != "" {
		return strings.TrimSpace(mountSource), nil
	}

	if _, err := os.Stat(mapperDevice); err == nil {
		return mapperDevice, nil
	} else if err != nil && !os.IsNotExist(err) {
		return "", err
	}

	return imagePath, nil
}

func mountedSourceForTarget(target string) (string, error) {
	output, err := runCommandWithOutput("findmnt", "-n", "-o", "SOURCE", "--target", target)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(output), nil
}

func parseSizeToBytes(raw string) (int64, error) {
	raw = strings.TrimSpace(raw)
	matches := sizePattern.FindStringSubmatch(raw)
	if len(matches) != 3 {
		return 0, fmt.Errorf("expected format like 10G, 10240M, or bytes")
	}

	value, err := strconv.ParseFloat(matches[1], 64)
	if err != nil {
		return 0, fmt.Errorf("invalid numeric value")
	}
	if value <= 0 {
		return 0, fmt.Errorf("size must be greater than zero")
	}

	multiplier, err := unitMultiplier(matches[2])
	if err != nil {
		return 0, err
	}

	bytesFloat := value * float64(multiplier)
	maxInt64 := float64(int64(^uint64(0) >> 1))
	if bytesFloat > maxInt64 {
		return 0, fmt.Errorf("size is too large")
	}

	return int64(math.Ceil(bytesFloat)), nil
}

func unitMultiplier(rawUnit string) (int64, error) {
	unit := strings.ToLower(strings.TrimSpace(rawUnit))
	switch unit {
	case "", "b":
		return 1, nil
	case "k", "kb":
		return 1000, nil
	case "m", "mb":
		return 1000 * 1000, nil
	case "g", "gb":
		return 1000 * 1000 * 1000, nil
	case "t", "tb":
		return 1000 * 1000 * 1000 * 1000, nil
	case "p", "pb":
		return 1000 * 1000 * 1000 * 1000 * 1000, nil
	case "ki", "kib":
		return 1024, nil
	case "mi", "mib":
		return 1024 * 1024, nil
	case "gi", "gib":
		return 1024 * 1024 * 1024, nil
	case "ti", "tib":
		return 1024 * 1024 * 1024 * 1024, nil
	case "pi", "pib":
		return 1024 * 1024 * 1024 * 1024 * 1024, nil
	default:
		return 0, fmt.Errorf("unsupported size unit '%s'", rawUnit)
	}
}

func GetVolumeStats(name, baseDir string) (*VolumeStats, error) {
	volumePath := filepath.Join(baseDir, name)
	dataPath := filepath.Join(volumePath, "_data")

	output, err := runCommandWithOutput("df", "-h", dataPath)
	if err != nil {
		return nil, fmt.Errorf("df command failed: %v", err)
	}

	lines := strings.Split(output, "\n")
	if len(lines) < 2 {
		return nil, fmt.Errorf("invalid df output")
	}

	fields := strings.Fields(lines[1])
	if len(fields) < 6 {
		return nil, fmt.Errorf("invalid df output fields")
	}

	stats := &VolumeStats{
		Name:      name,
		Size:      formatSize(fields[1]),
		Used:      formatSize(fields[2]),
		Available: formatSize(fields[3]),
		Usage:     fields[4],
		MountPath: fields[5],
	}

	return stats, nil
}

func formatSize(size string) string {
	if len(size) < 1 {
		return size
	}
	lastChar := size[len(size)-1]
	if (lastChar >= 'A' && lastChar <= 'Z') || (lastChar >= 'a' && lastChar <= 'z') {
		value := size[:len(size)-1]
		return value + " " + string(lastChar) + "B"
	}
	return size
}

func GetAllVolumes(baseDir string) ([]*VolumeStats, error) {
	files, err := os.ReadDir(baseDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read base directory: %v", err)
	}

	var volumes []*VolumeStats
	for _, file := range files {
		if file.IsDir() {
			stats, err := GetVolumeStats(file.Name(), baseDir)
			if err != nil {
				log.Printf("failed to get stats for %s: %v", file.Name(), err)
				continue
			}
			volumes = append(volumes, stats)
		}
	}

	return volumes, nil
}
