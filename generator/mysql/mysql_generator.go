package mysql

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/go-jet/jet/v2/generator/metadata"
	"github.com/go-jet/jet/v2/generator/template"
	"github.com/go-jet/jet/v2/internal/utils"
	"github.com/go-jet/jet/v2/internal/utils/throw"
	"github.com/go-jet/jet/v2/mysql"
	mysqldr "github.com/go-sql-driver/mysql"
)

// DBConnection contains MySQL connection details
type DBConnection struct {
	Host     string
	Port     int
	User     string
	Password string
	Params   string

	DBName string
}

// Generate generates jet files at destination dir from database connection details
func Generate(destDir string, dbConn DBConnection, generatorTemplate ...template.Template) (err error) {
	defer utils.ErrorCatch(&err)

	connectionString := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", dbConn.User, dbConn.Password, dbConn.Host, dbConn.Port, dbConn.DBName)
	if dbConn.Params != "" {
		connectionString += "?" + dbConn.Params
	}

	db := openConnection(connectionString)
	defer utils.DBClose(db)

	generate(db, dbConn.DBName, destDir, generatorTemplate...)

	return nil
}

// GenerateDSN opens connection via DSN string and does everything what Generate does.
func GenerateDSN(dsn, destDir string, templates ...template.Template) (err error) {
	defer utils.ErrorCatch(&err)

	// Special case for go mysql driver. It does not understand schema,
	// so we need to trim it before passing to generator
	// https://github.com/go-sql-driver/mysql#dsn-data-source-name
	idx := strings.Index(dsn, "://")
	if idx != -1 {
		dsn = dsn[idx+len("://"):]
	}

	cfg, err := mysqldr.ParseDSN(dsn)
	throw.OnError(err)
	if cfg.DBName == "" {
		panic("database name is required")
	}

	db := openConnection(dsn)
	defer utils.DBClose(db)

	generate(db, cfg.DBName, destDir, templates...)

	return nil
}

func openConnection(connectionString string) *sql.DB {
	fmt.Println("Connecting to MySQL database...")
	db, err := sql.Open("mysql", connectionString)
	throw.OnError(err)

	err = db.Ping()
	throw.OnError(err)

	return db
}

func generate(db *sql.DB, dbName, destDir string, templates ...template.Template) {
	fmt.Println("Retrieving database information...")
	// No schemas in MySQL
	schemaMetaData := metadata.GetSchema(db, &mySqlQuerySet{}, dbName)

	genTemplate := template.Default(mysql.Dialect)
	if len(templates) > 0 {
		genTemplate = templates[0]
	}

	template.ProcessSchema(destDir, schemaMetaData, genTemplate)
}
