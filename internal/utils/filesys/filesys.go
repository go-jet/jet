package filesys

import (
	"errors"
	"fmt"
	"go/format"
	"os"
	"path/filepath"
	"strings"
)

// FormatAndSaveGoFile saves go file at folder dir, with name fileName and contents text.
func FormatAndSaveGoFile(dirPath, fileName string, text []byte) error {
	newGoFilePath := filepath.Join(dirPath, fileName)

	if !strings.HasSuffix(newGoFilePath, ".go") {
		newGoFilePath += ".go"
	}

	file, err := os.Create(newGoFilePath) // #nosec 304

	if err != nil {
		return err
	}

	defer file.Close()

	p, err := format.Source(text)

	// if there is a format error we will write unformulated text for debug purposes
	if err != nil {
		_, writeErr := file.Write(text)
		if writeErr != nil {
			return errors.Join(writeErr, fmt.Errorf("failed to format '%s', check '%s' for syntax errors: %w", fileName, newGoFilePath, err))
		}
		return fmt.Errorf("failed to format '%s', check '%s' for syntax errors: %w", fileName, newGoFilePath, err)
	}

	_, err = file.Write(p)
	if err != nil {
		return fmt.Errorf("failed to save '%s' file: %w", newGoFilePath, err)
	}

	return nil
}

// EnsureDirPathExist ensures dir path exists. If path does not exist, creates new path.
func EnsureDirPathExist(dirPath string) error {
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		err := os.MkdirAll(dirPath, 0o750)

		if err != nil {
			return fmt.Errorf("can't create directory - %s: %w", dirPath, err)
		}
	}

	return nil
}

// RemoveDir deletes everything at folder dir.
func RemoveDir(dir string) error {
	exist, err := DirExists(dir)

	if err != nil {
		return err
	}

	if exist {
		err := os.RemoveAll(dir)

		if err != nil {
			return err
		}
	}

	return nil
}

// DirExists checks if folder at path exist.
func DirExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}
