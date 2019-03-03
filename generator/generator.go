package generator

import (
	"database/sql"
	_ "github.com/lib/pq"
	"github.com/serenize/snaker"
	"os"
)

type DbConnectInfo struct {
	host     string
	port     int
	user     string
	password string
	dbname   string
}

func Generate(folderPath string, connectString string, databaseName, schemaName string) error {
	if _, err := os.Stat(folderPath); os.IsNotExist(err) {
		err := os.Mkdir(folderPath, os.ModePerm)

		if err != nil {
			return err
		}
	}

	db, err := sql.Open("postgres", connectString)
	if err != nil {
		return err
	}
	defer db.Close()

	err = db.Ping()

	if err != nil {
		return err
	}

	tables, err := getTablesInfo(db, schemaName)

	if err != nil {
		return err
	}

	for _, table := range tables {
		err = generateSqlBuilderModel(databaseName, schemaName, table, folderPath)

		if err != nil {
			return err
		}
	}

	return nil
}

type TableInfo struct {
	Name    string
	Columns []ColumnInfo
}

func getTablesInfo(db *sql.DB, schemaName string) ([]TableInfo, error) {
	tableNames, err := getListOfTables(db, schemaName)

	if err != nil {
		return nil, err
	}

	tables := []TableInfo{}
	for _, tableName := range tableNames {
		columns, err := getColumnInfos(db, tableName)

		if err != nil {
			return nil, err
		}

		tables = append(tables, TableInfo{tableName, columns})
	}

	return tables, nil
}

func getListOfTables(db *sql.DB, schemaName string) ([]string, error) {

	rows, err := db.Query(`
SELECT table_name FROM information_schema.tables
where table_schema = $1 and table_type = 'BASE TABLE';`, schemaName)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tables := []string{}
	for rows.Next() {
		var table string
		err = rows.Scan(&table)
		if err != nil {
			return nil, err
		}

		tables = append(tables, table)
	}

	err = rows.Err()

	if err != nil {
		return nil, err
	}

	return tables, nil
}

type ColumnInfo struct {
	Name       string
	IsNullable bool
	DataType   string
}

func (c *ColumnInfo) CamelCaseName() string {
	return snaker.SnakeToCamel(c.Name)
}

func getColumnInfos(db *sql.DB, tableName string) ([]ColumnInfo, error) {

	query := `
SELECT column_name, is_nullable, data_type 
FROM information_schema.columns
where table_name = $1
order by ordinal_position;`

	//fmt.Println(query)

	rows, err := db.Query(query, &tableName)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	ret := []ColumnInfo{}

	for rows.Next() {
		columnInfo := ColumnInfo{}
		var isNullable string
		err := rows.Scan(&columnInfo.Name, &isNullable, &columnInfo.DataType)

		columnInfo.IsNullable = isNullable == "YES"

		if err != nil {
			return nil, err
		}

		ret = append(ret, columnInfo)
	}

	err = rows.Err()

	if err != nil {
		return nil, err
	}

	return ret, nil
}
