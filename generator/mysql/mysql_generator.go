package mysql

import (
	"database/sql"
	"fmt"
	"github.com/go-jet/jet"
	"github.com/go-jet/jet/generator/internal/metadata"
	"github.com/go-jet/jet/generator/internal/template"
	"path"
)

type DBConnection struct {
	Host     string
	Port     int
	User     string
	Password string
	SslMode  string
	Params   string

	DBName string
}

// Generate generates jet files at destination dir from database connection details
func Generate(destDir string, dbConn DBConnection) error {
	db, err := openConnection(dbConn)
	if err != nil {
		return err
	}
	defer db.Close()

	fmt.Println("Retrieving database information...")
	// No schemas in MySQL
	dbInfo, err := metadata.GetSchemaInfo(db, dbConn.DBName, &metadata.MySqlQuerySet{})

	if err != nil {
		return err
	}

	genPath := path.Join(destDir, dbConn.DBName)

	err = template.GenerateFiles(genPath, dbInfo.TableInfos, dbInfo.EnumInfos, jet.MySQL)

	if err != nil {
		return err
	}

	return nil
}

// TODO reuse
func openConnection(dbConn DBConnection) (*sql.DB, error) {
	var connString = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", dbConn.User, dbConn.Password, dbConn.Host, dbConn.Port, dbConn.DBName)
	db, err := sql.Open("mysql", connString)

	if err != nil {
		return nil, err
	}

	err = db.Ping()

	if err != nil {
		return nil, err
	}

	return db, nil
}
