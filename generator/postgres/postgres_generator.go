package postgres

import (
	"database/sql"
	"fmt"
	"github.com/go-jet/jet/v2/generator/internal/metadata"
	"github.com/go-jet/jet/v2/generator/internal/template"
	"github.com/go-jet/jet/v2/internal/utils"
	"github.com/go-jet/jet/v2/postgres"
	"path"
	"strconv"
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
	connectionString := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s %s",
		dbConn.Host, strconv.Itoa(dbConn.Port), dbConn.User, dbConn.Password, dbConn.DBName, dbConn.SslMode, dbConn.Params)

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
