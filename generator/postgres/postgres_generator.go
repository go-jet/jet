package postgres

import (
	"database/sql"
	"fmt"
	"net/url"
	"path/filepath"
	"strconv"

	"github.com/go-jet/jet/v2/generator/metadata"
	"github.com/go-jet/jet/v2/generator/template"
	"github.com/go-jet/jet/v2/postgres"
)

// DBConnection contains postgres connection details
type DBConnection struct {
	Host string
	Port int
	User string
	// #nosec G117 -- password is used only for the local development
	Password string
	SslMode  string
	Params   string

	DBName     string
	SchemaName string
}

// Generate generates jet files at destination dir from database connection details
func Generate(destDir string, dbConn DBConnection, genTemplate ...template.Template) (err error) {
	dsn := fmt.Sprintf("postgresql://%s:%s@%s:%s/%s?sslmode=%s",
		url.PathEscape(dbConn.User),
		url.PathEscape(dbConn.Password),
		dbConn.Host,
		strconv.Itoa(dbConn.Port),
		url.PathEscape(dbConn.DBName),
		dbConn.SslMode,
	)

	return GenerateDSN(dsn, dbConn.SchemaName, destDir, genTemplate...)
}

// GenerateDSN generates jet files using dsn connection string
func GenerateDSN(dsn, schema, destDir string, templates ...template.Template) error {
	_, err := url.Parse(dsn)
	if err != nil {
		return fmt.Errorf("failed to parse as DSN: %w", err)
	}

	db, err := openConnection(dsn)
	if err != nil {
		return fmt.Errorf("failed to open db connection: %w", err)
	}
	defer db.Close()

	var dbName string
	err = db.QueryRow("SELECT current_database()").Scan(&dbName)
	if err != nil {
		return fmt.Errorf("failed to get current database name: %w", err)
	}
	if dbName == "" {
		return fmt.Errorf("database name is required")
	}

	fmt.Println("Retrieving schema information...")
	return GenerateDB(db, schema, filepath.Join(destDir, dbName), templates...)
}

// GenerateDB generates jet files using the provided *sql.DB
func GenerateDB(db *sql.DB, schema, destDir string, templates ...template.Template) error {
	generatorTemplate := template.Default(postgres.Dialect)
	if len(templates) > 0 {
		generatorTemplate = templates[0]
	}

	schemaMetadata, err := metadata.GetSchema(db, &postgresQuerySet{}, schema)
	if err != nil {
		return fmt.Errorf("failed to get '%s' schema metadata: %w", schema, err)
	}

	err = template.ProcessSchema(destDir, schemaMetadata, generatorTemplate)
	if err != nil {
		return fmt.Errorf("failed to generate schema %s: %w", schemaMetadata.Name, err)
	}

	return nil
}

func openConnection(dsn string) (*sql.DB, error) {
	fmt.Println("Connecting to postgres database...")

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open db connection: %w", err)
	}

	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return db, nil
}
