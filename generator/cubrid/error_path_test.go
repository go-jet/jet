package cubrid

import (
	"testing"

	cubriddriver "github.com/search5/cubrid-go"
)

// TestGenerateDSN_MissingDBName covers the extractDBName error path in GenerateDSN.
func TestGenerateDSN_MissingDBName(t *testing.T) {
	err := GenerateDSN("cubrid://dba:@localhost:33000", t.TempDir())
	if err == nil {
		t.Fatal("expected error for DSN without database name, got nil")
	}
}

// TestGeneratePool_EmptyDSN covers the NewPool error path in GeneratePool.
func TestGeneratePool_EmptyDSN(t *testing.T) {
	err := GeneratePool(cubriddriver.PoolConfig{DSN: ""}, "testdb", t.TempDir())
	if err == nil {
		t.Fatal("expected error for empty pool DSN, got nil")
	}
}

// TestGenerateHA_InvalidDSN covers the NewHACluster error path in GenerateHA.
func TestGenerateHA_InvalidDSN(t *testing.T) {
	err := GenerateHA(cubriddriver.HAConfig{DSN: ""}, "testdb", t.TempDir(), false)
	if err == nil {
		t.Fatal("expected error for empty HA DSN, got nil")
	}
}

// TestGenerate_ConnectionRefused covers the openConnection error path.
// Uses a port guaranteed to refuse connections so the test is fast.
func TestGenerate_ConnectionRefused(t *testing.T) {
	err := Generate(t.TempDir(), DBConnection{
		Host: "127.0.0.1", Port: 1,
		User: "dba", Password: "", DBName: "testdb",
	})
	if err == nil {
		t.Fatal("expected error for unreachable CUBRID host, got nil")
	}
}
