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
	mariaImageVersion = "docker.io/mariadb:10.3.32"
	mariaImageInitDb  = "/docker-entrypoint-initdb.d"
	mariaTestDataPath = "./testdata/init/mysql"
	mariaDefaultPort  = "3306/tcp"
)

// SetupWithMariaDB spins up a DB Container and returns the host and port
func SetupWithMariaDB(testRoot string) (string, int, context.CancelFunc) {
	localBase := filepath.Join(testRoot, mariaTestDataPath)

	files, err := os.ReadDir(localBase)
	if err != nil {
		log.Fatal("test data for maria is missing")
	}
	initFiles := make([]testcontainers.ContainerFile, 0)
	for _, file := range files {
		r, openFileErr := os.Open(filepath.Join(localBase, file.Name()))
		if openFileErr != nil {
			log.Fatalf("unable to open file %s", file.Name())
		}

		item := testcontainers.ContainerFile{
			Reader:            r,
			ContainerFilePath: filepath.Join(mariaImageInitDb, file.Name()),
			FileMode:          0o777,
		}

		defer r.Close()
		initFiles = append(initFiles, item)
	}

	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        mariaImageVersion,
		ExposedPorts: []string{mariaDefaultPort},
		Env: map[string]string{
			"MYSQL_ROOT_PASSWORD": "jet",
			"MYSQL_USER":          "jet",
			"MYSQL_PASSWORD":      "jet",
		},
		Cmd:        []string{"--default-authentication-plugin=maria_native_password", "--log_bin_trust_function_creators=1"},
		WaitingFor: wait.ForListeningPort(mariaDefaultPort),
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
		log.Fatal("unable to determine maria hostname")
	}
	port, err := dbContainer.MappedPort(context.Background(), mariaDefaultPort)
	if err != nil {
		log.Fatal("no valid maria tcp port found")
	}

	newPort, err := getNumericPort(string(port))
	if err != nil {
		log.Fatal("no valid maria tcp port found")
	}

	return host, newPort, cancel
}
