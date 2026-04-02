// Package cubrid provides a go-jet code generator for CUBRID databases.
//
// It uses the CCI protocol CAS_FC_SCHEMA_INFO function via the cubrid-go
// driver to query the CUBRID system catalog and generate type-safe Go code.
package cubrid

import (
	"database/sql"
	"fmt"

	cubriddriver "github.com/search5/cubrid-go"
	"github.com/go-jet/jet/v2/cubrid"
	"github.com/go-jet/jet/v2/generator/metadata"
	"github.com/go-jet/jet/v2/generator/template"
)

const cubridMaxConns = 10

// DBConnection contains CUBRID connection details.
type DBConnection struct {
	Host     string
	Port     int
	User     string
	Password string
	DBName   string
}

// Generate generates jet files at destination dir from database connection details.
func Generate(destDir string, dbConn DBConnection, generatorTemplate ...template.Template) error {
	dsn := fmt.Sprintf("cubrid://%s:%s@%s:%d/%s",
		dbConn.User, dbConn.Password, dbConn.Host, dbConn.Port, dbConn.DBName)

	db, err := openConnection(dsn)
	if err != nil {
		return fmt.Errorf("failed to open db connection: %w", err)
	}
	defer db.Close()

	return GenerateDB(db, dbConn.DBName, destDir, generatorTemplate...)
}

// GenerateDSN opens connection via DSN string and generates jet files.
// DSN format: cubrid://user:password@host:port/dbname
func GenerateDSN(dsn, destDir string, templates ...template.Template) error {
	dbName, err := extractDBName(dsn)
	if err != nil {
		return err
	}

	db, err := openConnection(dsn)
	if err != nil {
		return fmt.Errorf("failed to open db connection: %w", err)
	}
	defer db.Close()

	return GenerateDB(db, dbName, destDir, templates...)
}

// GenerateDB generates jet files using the provided *sql.DB.
func GenerateDB(db *sql.DB, dbName, destDir string, templates ...template.Template) error {
	fmt.Println("Retrieving CUBRID database information...")

	schemaMetaData, err := metadata.GetSchema(db, &cubridQuerySet{}, dbName)
	if err != nil {
		return fmt.Errorf("failed to get '%s' database metadata: %w", dbName, err)
	}

	genTemplate := template.Default(cubrid.Dialect)
	if len(templates) > 0 {
		genTemplate = templates[0]
	}

	err = template.ProcessSchema(destDir, schemaMetaData, genTemplate)
	if err != nil {
		return fmt.Errorf("failed to process '%s' database: %w", schemaMetaData.Name, err)
	}

	return nil
}

// GeneratePool generates jet files using a CUBRID-aware connection pool.
// The pool provides health validation and broker failover handling.
func GeneratePool(config cubriddriver.PoolConfig, dbName, destDir string, templates ...template.Template) error {
	pool, err := cubriddriver.NewPool(config)
	if err != nil {
		return fmt.Errorf("failed to create pool: %w", err)
	}
	defer pool.Close()

	return GenerateDB(pool.DB(), dbName, destDir, templates...)
}

// GenerateHA generates jet files using an HA cluster connection.
// If readOnly is true, the generator reads from a standby broker.
func GenerateHA(config cubriddriver.HAConfig, dbName, destDir string, readOnly bool, templates ...template.Template) error {
	cluster, err := cubriddriver.NewHACluster(config)
	if err != nil {
		return fmt.Errorf("failed to create HA cluster: %w", err)
	}
	defer cluster.Close()

	return GenerateDB(cluster.DB(readOnly), dbName, destDir, templates...)
}

func openConnection(dsn string) (*sql.DB, error) {
	fmt.Println("Connecting to CUBRID database...")

	db, err := sql.Open("cubrid", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open cubrid connection: %w", err)
	}

	db.SetMaxOpenConns(cubridMaxConns)
	db.SetMaxIdleConns(cubridMaxConns)

	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return db, nil
}
