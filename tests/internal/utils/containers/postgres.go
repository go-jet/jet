package containers

import (
	"context"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"log"
	"log/slog"
	"os"
	"path/filepath"
)

const (
	postgresImageVersion = "docker.io/postgres:14.1"
	postgresImageInitDb  = "/docker-entrypoint-initdb.d"
	postgresTestDataPath = "./testdata/init/postgres"
	postgresDefaultPort  = "5432/tcp"
)

// SetupWithPostgres spins up a DB Container and returns the host and port
func SetupWithPostgres(testRoot string) (string, int, context.CancelFunc) {
	localBase := filepath.Join(testRoot, postgresTestDataPath)

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
			ContainerFilePath: filepath.Join(postgresImageInitDb, file.Name()),
			FileMode:          0o777,
		}

		defer r.Close()
		initFiles = append(initFiles, item)
	}

	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        postgresImageVersion,
		ExposedPorts: []string{postgresDefaultPort},
		Env: map[string]string{
			"POSTGRES_USER":     "jet",
			"POSTGRES_PASSWORD": "jet",
			"POSTGRES_DB":       "jetdb",
		},
		WaitingFor: wait.ForListeningPort(postgresDefaultPort),
		Files:      initFiles,
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
		slog.Info("Tearing down database testcontainer")
		if err := dbContainer.Terminate(ctx); err != nil {
			panic(err)
		}
	}
	host, err := dbContainer.Host(context.Background())
	if err != nil {
		log.Fatal("unable to determine postgres hostname")
	}
	port, err := dbContainer.MappedPort(context.Background(), postgresDefaultPort)
	if err != nil {
		log.Fatal("no valid postgres tcp port found")
	}

	newPort, err := getNumericPort(string(port))
	if err != nil {
		log.Fatal("no valid postgres tcp port found")
	}

	return host, newPort, cancel
}
