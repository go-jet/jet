package mysql

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/go-jet/jet/v2/generator/metadata"
	"github.com/go-jet/jet/v2/generator/template"
	"github.com/go-jet/jet/v2/mysql"
	mysqldr "github.com/go-sql-driver/mysql"
)

const mysqlMaxConns = 10

// DBConnection contains MySQL connection details
type DBConnection struct {
	// The server host.
	// Has no effect if LocalSocket is set.
	Host string

	// The server port.
	// Has no effect if LocalSocket is set.
	Port int

	// The local UNIX socket to connect to.
	// Overrides Host and Port if set.
	LocalSocket string

	User     string
	Password string
	Params   string

	DBName string
}

// Generate generates jet files at destination dir from database connection details
func Generate(destDir string, dbConn DBConnection, generatorTemplate ...template.Template) error {
	var network string
	var addr string
	if dbConn.LocalSocket == "" {
		network = "tcp"
		addr = fmt.Sprintf("%s:%d", dbConn.Host, dbConn.Port)
	} else {
		network = "unix"
		addr = dbConn.LocalSocket
	}

	connectionString := fmt.Sprintf("%s:%s@%s(%s)/%s", dbConn.User, dbConn.Password, network, addr, dbConn.DBName)
	if dbConn.Params != "" {
		connectionString += "?" + dbConn.Params
	}

	db, err := openConnection(connectionString)
	if err != nil {
		return fmt.Errorf("failed to open db connection: %w", err)
	}
	defer db.Close()

	err = GenerateDB(db, dbConn.DBName, destDir, generatorTemplate...)
	if err != nil {
		return err
	}

	return nil
}

// GenerateDSN opens connection via DSN string and does everything what Generate does.
func GenerateDSN(dsn, destDir string, templates ...template.Template) error {
	// Special case for go mysql driver. It does not understand schema,
	// so we need to trim it before passing to generator
	// https://github.com/go-sql-driver/mysql#dsn-data-source-name
	idx := strings.Index(dsn, "://")
	if idx != -1 {
		dsn = dsn[idx+len("://"):]
	}

	cfg, err := mysqldr.ParseDSN(dsn)
	if err != nil {
		return fmt.Errorf("failed to parse DSN: %w", err)
	}
	if cfg.DBName == "" {
		return errors.New("database name is required")
	}

	db, err := openConnection(dsn)
	if err != nil {
		return fmt.Errorf("failed to open db connection: %w", err)
	}
	defer db.Close()

	err = GenerateDB(db, cfg.DBName, destDir, templates...)
	if err != nil {
		return fmt.Errorf("failed to generate: %w", err)
	}

	return nil
}

func openConnection(connectionString string) (*sql.DB, error) {
	fmt.Println("Connecting to MySQL database...")
	db, err := sql.Open("mysql", connectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to open mysql connection: %w", err)
	}

	db.SetMaxOpenConns(mysqlMaxConns)
	db.SetMaxIdleConns(mysqlMaxConns)

	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return db, nil
}

// GenerateDB generates jet files using the provided *sql.DB
func GenerateDB(db *sql.DB, dbName, destDir string, templates ...template.Template) error {
	fmt.Println("Retrieving database information...")
	// No schemas in MySQL
	schemaMetaData, err := metadata.GetSchema(db, &mySqlQuerySet{}, dbName)
	if err != nil {
		return fmt.Errorf("failed to get '%s' database metadata: %w", dbName, err)
	}

	genTemplate := template.Default(mysql.Dialect)
	if len(templates) > 0 {
		genTemplate = templates[0]
	}

	err = template.ProcessSchema(destDir, schemaMetaData, genTemplate)
	if err != nil {
		return fmt.Errorf("failed to process '%s' database: %w", schemaMetaData.Name, err)
	}

	return nil
}
