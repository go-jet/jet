//go:build integration

package cubrid

import (
	"os"
	"path/filepath"
	"testing"

	cubriddriver "github.com/search5/cubrid-go"
)

func TestIntegrationGeneratePool(t *testing.T) {
	// Ensure test tables exist.
	db := getTestDB(t)
	setupTestTable(t, db)

	destDir := t.TempDir()

	err := GeneratePool(cubriddriver.PoolConfig{
		DSN:     getTestDSN(),
		MaxOpen: 5,
		MaxIdle: 2,
	}, "cubdb", destDir)
	if err != nil {
		t.Fatalf("GeneratePool: %v", err)
	}

	// Verify generated structure.
	modelDir := filepath.Join(destDir, "cubdb", "model")
	if _, err := os.Stat(modelDir); os.IsNotExist(err) {
		t.Error("model directory was not created via GeneratePool")
	}

	entries, err := os.ReadDir(modelDir)
	if err != nil {
		t.Fatalf("read model dir: %v", err)
	}
	fileNames := make(map[string]bool)
	for _, e := range entries {
		fileNames[e.Name()] = true
	}
	if !fileNames["jet_test_users.go"] {
		t.Errorf("jet_test_users.go not found via GeneratePool, files: %v", fileNames)
	}
	t.Log("go-jet + Pool: code generation succeeded")
}

func TestIntegrationGenerateHA(t *testing.T) {
	// Ensure test tables exist.
	db := getTestDB(t)
	setupTestTable(t, db)

	destDir := t.TempDir()

	err := GenerateHA(cubriddriver.HAConfig{
		DSN:              getTestDSN() + "?ha=true",
		MaxOpenPerBroker: 3,
	}, "cubdb", destDir, false)
	if err != nil {
		t.Fatalf("GenerateHA: %v", err)
	}

	modelDir := filepath.Join(destDir, "cubdb", "model")
	if _, err := os.Stat(modelDir); os.IsNotExist(err) {
		t.Error("model directory was not created via GenerateHA")
	}

	entries, err := os.ReadDir(modelDir)
	if err != nil {
		t.Fatalf("read model dir: %v", err)
	}
	fileNames := make(map[string]bool)
	for _, e := range entries {
		fileNames[e.Name()] = true
	}
	if !fileNames["jet_test_users.go"] {
		t.Errorf("jet_test_users.go not found via GenerateHA, files: %v", fileNames)
	}
	t.Log("go-jet + HA: code generation succeeded")
}
