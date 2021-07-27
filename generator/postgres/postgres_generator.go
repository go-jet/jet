package postgres

import (
	"database/sql"
	"fmt"
	"github.com/go-jet/jet/v2/generator/metadata"
	"github.com/go-jet/jet/v2/generator/template"
	"github.com/go-jet/jet/v2/internal/utils"
	"github.com/go-jet/jet/v2/internal/utils/throw"
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
func Generate(destDir string, dbConn DBConnection, genTemplate ...template.Template) (err error) {
	defer utils.ErrorCatch(&err)

	db := openConnection(dbConn)
	defer utils.DBClose(db)

	fmt.Println("Retrieving schema information...")

	generatorTemplate := template.Default(postgres.Dialect)
	if len(genTemplate) > 0 {
		generatorTemplate = genTemplate[0]
	}

	schemaMetadata := metadata.GetSchema(db, &postgresQuerySet{}, dbConn.SchemaName)

	dirPath := path.Join(destDir, dbConn.DBName)

	template.ProcessSchema(dirPath, schemaMetadata, generatorTemplate)

	return
}

func openConnection(dbConn DBConnection) *sql.DB {
	connectionString := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s %s",
		dbConn.Host, strconv.Itoa(dbConn.Port), dbConn.User, dbConn.Password, dbConn.DBName, dbConn.SslMode, dbConn.Params)

	fmt.Println("Connecting to postgres database: " + connectionString)

	db, err := sql.Open("postgres", connectionString)
	throw.OnError(err)

	err = db.Ping()
	throw.OnError(err)

	return db
}
