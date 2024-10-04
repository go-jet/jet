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
	mysqlImageVersion = "docker.io/mysql:8.0"
	mysqlImageInitDb  = "/docker-entrypoint-initdb.d"
	mysqlTestDataPath = "./testdata/init/mysql"
	mysqlDefaultPort  = "3306/tcp"
)

// SetupWithMySQL spins up a DB Container and returns the host and port
func SetupWithMySQL(testRoot string) (string, int, context.CancelFunc) {
	localBase := filepath.Join(testRoot, mysqlTestDataPath)

	files, err := os.ReadDir(localBase)
	if err != nil {
		log.Fatal("test data for mysql is missing")
	}
	initFiles := make([]testcontainers.ContainerFile, 0)
	for _, file := range files {
		r, openFileErr := os.Open(filepath.Join(localBase, file.Name()))
		if openFileErr != nil {
			log.Fatalf("unable to open file %s", file.Name())
		}

		item := testcontainers.ContainerFile{
			Reader:            r,
			ContainerFilePath: filepath.Join(mysqlImageInitDb, file.Name()),
			FileMode:          0o777,
		}

		defer r.Close()
		initFiles = append(initFiles, item)
	}

	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        mysqlImageVersion,
		ExposedPorts: []string{mysqlDefaultPort},
		Env: map[string]string{
			"MYSQL_ROOT_PASSWORD": "jet",
			"MYSQL_USER":          "jet",
			"MYSQL_PASSWORD":      "jet",
		},
		Cmd:        []string{"--default-authentication-plugin=mysql_native_password", "--log_bin_trust_function_creators=1"},
		WaitingFor: wait.ForListeningPort(mysqlDefaultPort),
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
		log.Fatal("unable to determine mysql hostname")
	}
	port, err := dbContainer.MappedPort(context.Background(), mysqlDefaultPort)
	if err != nil {
		log.Fatal("no valid mysql tcp port found")
	}

	newPort, err := getNumericPort(string(port))
	if err != nil {
		log.Fatal("no valid mysql tcp port found")
	}

	return host, newPort, cancel
}
