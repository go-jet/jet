package mysql

import (
	"database/sql"
	"fmt"
	"github.com/go-jet/jet/v2/generator/internal/metadata"
	"github.com/go-jet/jet/v2/generator/internal/template"
	"github.com/go-jet/jet/v2/internal/utils"
	"github.com/go-jet/jet/v2/mysql"
	"path"
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
func Generate(destDir string, dbConn DBConnection) (err error) {
	defer utils.ErrorCatch(&err)

	db := openConnection(dbConn)
	defer utils.DBClose(db)

	fmt.Println("Retrieving database information...")
	// No schemas in MySQL
	dbInfo := metadata.GetSchemaMetaData(db, dbConn.DBName, &mySqlQuerySet{})

	genPath := path.Join(destDir, dbConn.DBName)

	template.GenerateFiles(genPath, dbInfo, mysql.Dialect)

	return nil
}

func openConnection(dbConn DBConnection) *sql.DB {
	var connectionString = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", dbConn.User, dbConn.Password, dbConn.Host, dbConn.Port, dbConn.DBName)
	if dbConn.Params != "" {
		connectionString += "?" + dbConn.Params
	}
	fmt.Println("Connecting to MySQL database: " + connectionString)
	db, err := sql.Open("mysql", connectionString)
	utils.PanicOnError(err)

	err = db.Ping()
	utils.PanicOnError(err)

	return db
}
