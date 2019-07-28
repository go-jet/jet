package postgres

import (
	"database/sql"
	"fmt"
	"github.com/go-jet/jet"
	"github.com/go-jet/jet/generator/internal/metadata"
	"github.com/go-jet/jet/generator/internal/template"
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
func Generate(destDir string, dbConn DBConnection) error {

	db, err := openConnection(dbConn)
	defer db.Close()

	if err != nil {
		return err
	}

	fmt.Println("Retrieving schema information...")
	schemaInfo, err := metadata.GetSchemaInfo(db, dbConn.SchemaName, &metadata.PostgresQuerySet{})

	if err != nil {
		return err
	}

	genPath := path.Join(destDir, dbConn.DBName, dbConn.SchemaName)

	err = template.GenerateFiles(genPath, schemaInfo.TableInfos, schemaInfo.EnumInfos, jet.PostgreSQL)

	if err != nil {
		return err
	}

	return nil
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
