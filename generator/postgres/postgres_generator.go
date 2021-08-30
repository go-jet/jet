package postgres

import (
	"database/sql"
	"fmt"
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

	connectionString := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s %s",
		dbConn.Host, strconv.Itoa(dbConn.Port), dbConn.User, dbConn.Password, dbConn.DBName, dbConn.SslMode, dbConn.Params)

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
