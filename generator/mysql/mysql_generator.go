package mysql

import (
	"database/sql"
	"fmt"
	"github.com/go-jet/jet/v2/generator/metadata"
	"github.com/go-jet/jet/v2/generator/template"
	"github.com/go-jet/jet/v2/internal/utils"
	"github.com/go-jet/jet/v2/internal/utils/throw"
	"github.com/go-jet/jet/v2/mysql"
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

	db := openConnection(dbConn)
	defer utils.DBClose(db)

	fmt.Println("Retrieving database information...")
	// No schemas in MySQL
	schemaMetaData := metadata.GetSchema(db, &mySqlQuerySet{}, dbConn.DBName)

	genTemplate := template.Default(mysql.Dialect)
	if len(generatorTemplate) > 0 {
		genTemplate = generatorTemplate[0]
	}

	template.ProcessSchema(destDir, schemaMetaData, genTemplate)

	return nil
}

func openConnection(dbConn DBConnection) *sql.DB {
	var connectionString = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", dbConn.User, dbConn.Password, dbConn.Host, dbConn.Port, dbConn.DBName)
	if dbConn.Params != "" {
		connectionString += "?" + dbConn.Params
	}
	fmt.Println("Connecting to MySQL database: " + connectionString)
	db, err := sql.Open("mysql", connectionString)
	throw.OnError(err)

	err = db.Ping()
	throw.OnError(err)

	return db
}
