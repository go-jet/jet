package containers

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/go-jet/jet/v2/generator/postgres"
	dbTools "github.com/go-jet/jet/v2/tests/internal/utils/db"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	Cockroach = "cockroach"
	//cockroachImageVersion = "cockroachdb/cockroach-unstable:v23.1.0-rc.2"
	cockroachImageVersion = "cockroachdb/cockroach:v23.1.27"
	cockroachImageInitDb  = "/docker-entrypoint-initdb.d"
	cockroachTestDataPath = "./testdata/init/cockroach"
	cockroachDefaultPort  = "26257/tcp"
)

// getNumericPort strips away the protocol and gets int value
func getNumericPort(port string) (int, error) {
	rawPort := strings.ReplaceAll(port, "/tcp", "")
	return strconv.Atoi(rawPort)
}

// SetupWithCockroach spins up a DB Container and returns the host and port
func SetupWithCockroach(testRoot string) (string, int, context.CancelFunc) {

	localBase := filepath.Join(testRoot, cockroachTestDataPath)

	files, err := os.ReadDir(localBase)
	if err != nil {
		log.Fatal("test data for postgres is missing")
	}
	initFiles := make([]testcontainers.ContainerFile, 0)
	for _, file := range files {
		r, openFileErr := os.Open(filepath.Join(localBase, file.Name()))
		if openFileErr != nil {
			log.Fatalf("unable to open file %s", file.Name())
		}

		item := testcontainers.ContainerFile{
			Reader:            r,
			ContainerFilePath: filepath.Join(cockroachImageInitDb, file.Name()),
			FileMode:          0o777,
		}

		defer r.Close()
		initFiles = append(initFiles, item)
	}

	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        cockroachImageVersion,
		ExposedPorts: []string{cockroachDefaultPort},
		Cmd:          []string{"start-single-node", "--accept-sql-without-tls"},
		Env: map[string]string{
			"COCKROACH_USER":     "jet",
			"COCKROACH_PASSWORD": "jet",
			"COCKROACH_DATABASE": "jetdb",
		},
		WaitingFor: wait.ForLog("end running init files from /docker-entrypoint-initdb.d").WithStartupTimeout(time.Minute * 10),
		//wait.ForListeningPort(cockroachDefaultPort),
		Files: initFiles,
	}
	dbContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Reuse:            false,
		Started:          true,
		ProviderType:     testcontainers.ProviderDocker,
	})

	if err != nil {
		log.Fatal("Failed to start testcontainer")
	}
	var cancel context.CancelFunc = func() {
		slog.Info("Tearing down dockroach database testcontainer")
		if err := dbContainer.Terminate(ctx); err != nil {
			panic(err)
		}
	}
	host, err := dbContainer.Host(context.Background())
	if err != nil {
		log.Fatal("unable to determine cockroach hostname")
	}
	port, err := dbContainer.MappedPort(context.Background(), cockroachDefaultPort)
	if err != nil {
		log.Fatal("no valid cockroach tcp port found")
	}
	newPort, err := getNumericPort(string(port))
	if err != nil {
		log.Fatal("no valid postgres tcp port found")
	}

	return host, newPort, cancel
}

// InitCockroachDb Deprecated No longer needed
func InitCockroachDb(testRoot, connectionString string) error {
	dbType := Cockroach
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		return fmt.Errorf("failed to open '%s' db connection '%s': %w", dbType, connectionString, err)
	}
	defer db.Close()

	schemaNames := []string{
		"northwind",
		"dvds",
		"test_sample",
		"chinook",
		"chinook2",
	}

	for _, schemaName := range schemaNames {
		fmt.Println("\nInitializing", schemaName, "schema...")

		err = dbTools.ExecFile(db, fmt.Sprintf(filepath.Join(testRoot, "./testdata/init/%s/%s.sql"), dbType, schemaName))
		if err != nil {
			slog.Error("failed to execute sql file", slog.Any("err", err))
			return fmt.Errorf("failed to execute sql file: %w", err)
		}

		err = postgres.GenerateDSN(connectionString, schemaName, "./.gentestdata")
		if err != nil {
			slog.Error("failed to generate jet types", slog.Any("err", err))
			return fmt.Errorf("failed to generate jet types: %w", err)
		}
	}

	return nil
}
