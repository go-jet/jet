package file

import (
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"testing"
)

// Exists expects file to exist on path constructed from pathElems and returns content of the file
func Exists(t *testing.T, pathElems ...string) (fileContent string) {
	modelFilePath := filepath.Join(pathElems...)
	file, err := os.ReadFile(modelFilePath) // #nosec G304
	require.Nil(t, err)
	require.NotEmpty(t, file)
	return string(file)
}

// NotExists expects file not to exist on path constructed from pathElems
func NotExists(t *testing.T, pathElems ...string) {
	modelFilePath := filepath.Join(pathElems...)
	_, err := os.ReadFile(modelFilePath) // #nosec G304
	require.True(t, os.IsNotExist(err))
}
