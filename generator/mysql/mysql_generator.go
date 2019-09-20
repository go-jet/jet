package mysql

import (
	"database/sql"
	"fmt"
	"github.com/go-jet/jet/generator/internal/metadata"
	"github.com/go-jet/jet/generator/internal/template"
	"github.com/go-jet/jet/internal/utils"
	"github.com/go-jet/jet/mysql"
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
func Generate(destDir string, dbConn DBConnection) error {
	db, err := openConnection(dbConn)
	if err != nil {
		return err
	}
	defer utils.DBClose(db)

	fmt.Println("Retrieving database information...")
	// No schemas in MySQL
	dbInfo, err := metadata.GetSchemaMetaData(db, dbConn.DBName, &mySqlQuerySet{})

	if err != nil {
		return err
	}

	genPath := path.Join(destDir, dbConn.DBName)

	err = template.GenerateFiles(genPath, dbInfo, mysql.Dialect)

	if err != nil {
		return err
	}

	return nil
}

func openConnection(dbConn DBConnection) (*sql.DB, error) {
	var connectionString = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", dbConn.User, dbConn.Password, dbConn.Host, dbConn.Port, dbConn.DBName)
	if dbConn.Params != "" {
		connectionString += "?" + dbConn.Params
	}
	db, err := sql.Open("mysql", connectionString)

	fmt.Println("Connecting to MySQL database: " + connectionString)

	if err != nil {
		return nil, err
	}

	err = db.Ping()

	if err != nil {
		return nil, err
	}

	return db, nil
}
