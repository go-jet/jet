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
	defer utils.ErrorCatch(&err)

	if dbConfig.SchemaName == "" {
		dbConfig.SchemaName = "public"
	}
	connectionString := fmt.Sprintf("postgresql://%s:%s@%s:%s/%s?sslmode=%s&search_path=%s",
		dbConn.User, url.QueryEscape(dbConn.Password), dbConn.Host, strconv.Itoa(dbConn.Port), dbConn.DBName, dbConn.SslMode, dbConn.SchemaName)

	db := openConnection(connectionString)
	defer utils.DBClose(db)

	generate(db, dbConn.DBName, dbConn.SchemaName, destDir, genTemplate...)

	return
}

func GenerateDSN(dsn, schema, destDir string, templates ...template.Template) (err error) {
	defer utils.ErrorCatch(&err)

	cfg, err := pgconn.ParseConfig(dsn)
	throw.OnError(err)
	if cfg.Database == "" {
		panic("database name is required")
	}
	db := openConnection(dsn)
	defer utils.DBClose(db)

	generate(db, cfg.Database, schema, destDir, templates...)

	return
}

func openConnection(dsn string) *sql.DB {
	fmt.Println("Connecting to postgres database: " + dsn)

	db, err := sql.Open("postgres", dsn)
	throw.OnError(err)

	err = db.Ping()
	throw.OnError(err)

	return db
}

func generate(db *sql.DB, dbName, schema, destDir string, templates ...template.Template) {
	fmt.Println("Retrieving schema information...")
	generatorTemplate := template.Default(postgres.Dialect)
	if len(templates) > 0 {
		generatorTemplate = templates[0]
	}

	schemaMetadata := metadata.GetSchema(db, &postgresQuerySet{}, schema)

	dirPath := path.Join(destDir, dbName)

	template.ProcessSchema(dirPath, schemaMetadata, generatorTemplate)
}
