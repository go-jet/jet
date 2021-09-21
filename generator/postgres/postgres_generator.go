package postgres

import (
	"database/sql"
	"fmt"
	"net/url"
	"path"
	"strconv"

	"github.com/go-jet/jet/v2/generator/internal/metadata"
	"github.com/go-jet/jet/v2/generator/internal/template"
	"github.com/go-jet/jet/v2/internal/utils"
	"github.com/go-jet/jet/v2/postgres"
)

// DBConnection contains postgres connection details
type DBConnection struct {
	Host     string
	Port     int
	User     string
	Password string
	SslMode  string
	Params   string

	DBName     string
	SchemaName string
}

// Generate generates jet files at destination dir from database connection details
func Generate(destDir string, dbConn DBConnection) (err error) {
	defer utils.ErrorCatch(&err)

	db, err := openConnection(dbConn)
	utils.PanicOnError(err)
	defer utils.DBClose(db)

	fmt.Println("Retrieving schema information...")
	schemaInfo := metadata.GetSchemaMetaData(db, dbConn.SchemaName, &postgresQuerySet{})

	genPath := path.Join(destDir, dbConn.DBName, dbConn.SchemaName)
	template.GenerateFiles(genPath, schemaInfo, postgres.Dialect)

	return
}

func openConnection(dbConn DBConnection) (*sql.DB, error) {
	if dbConfig.SchemaName == "" {
		dbConfig.SchemaName = "public"
	}
	connectionString := fmt.Sprintf("postgresql://%s:%s@%s:%s/%s?sslmode=%s&search_path=%s",
		dbConn.User, url.QueryEscape(dbConn.Password), dbConn.Host, strconv.Itoa(dbConn.Port), dbConn.DBName, dbConn.SslMode, dbConn.SchemaName)

	fmt.Println("Connecting to postgres database: " + connectionString)

	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		return nil, err
	}

	err = db.Ping()

	if err != nil {
		return nil, err
	}

	return db, nil
}
