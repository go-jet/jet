package repo

import (
	"os/exec"
	"path/filepath"
	"strings"
)

// GetRootDirPath will return this repo full dir path
func GetRootDirPath() string {
	cmd := exec.Command("git", "rev-parse", "--show-toplevel")
	byteArr, err := cmd.Output()
	if err != nil {
		panic(err)
	}

	return strings.TrimSpace(string(byteArr))
}

// GetTestsDirPath will return tests folder full path
func GetTestsDirPath() string {
	return filepath.Join(GetRootDirPath(), "tests")
}

// GetTestsFilePath will return full file path of the file in the tests folder
func GetTestsFilePath(subPath string) string {
	return filepath.Join(GetTestsDirPath(), subPath)
}

// GetTestDataFilePath will return full file path of the file in the testdata folder
func GetTestDataFilePath(subPath string) string {
	return filepath.Join(GetTestsDirPath(), "testdata", subPath)
}
