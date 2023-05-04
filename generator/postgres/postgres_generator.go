package postgres

import (
	"database/sql"
	"fmt"
	"net/url"
	"path"
	"strconv"

	"github.com/go-jet/jet/v2/generator/metadata"
	"github.com/go-jet/jet/v2/generator/template"
	"github.com/go-jet/jet/v2/internal/utils"
	"github.com/go-jet/jet/v2/internal/utils/throw"
	"github.com/go-jet/jet/v2/postgres"
	"github.com/jackc/pgconn"
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
func GenerateDSN(dsn, schema, destDir string, templates ...template.Template) (err error) {
	defer utils.ErrorCatch(&err)

	cfg, err := pgconn.ParseConfig(dsn)
	throw.OnError(err)
	if cfg.Database == "" {
		panic("database name is required")
	}
	db := openConnection(dsn)
	defer utils.DBClose(db)

	fmt.Println("Retrieving schema information...")
	generatorTemplate := template.Default(postgres.Dialect)
	if len(templates) > 0 {
		generatorTemplate = templates[0]
	}

	var isRedshift bool
	err = db.QueryRow("SELECT version() LIKE '%Redshift%';").Scan(&isRedshift)
	throw.OnError(err)

	var schemaMetadata metadata.Schema
	if isRedshift {
		schemaMetadata = metadata.GetSchema(db, &redshiftQuerySet{}, schema)
	} else {
		schemaMetadata = metadata.GetSchema(db, &postgresQuerySet{}, schema)
	}

	dirPath := path.Join(destDir, cfg.Database)

	template.ProcessSchema(dirPath, schemaMetadata, generatorTemplate)
	return
}

func openConnection(dsn string) *sql.DB {
	fmt.Println("Connecting to postgres database...")

	db, err := sql.Open("postgres", dsn)
	throw.OnError(err)

	err = db.Ping()
	throw.OnError(err)

	return db
}
