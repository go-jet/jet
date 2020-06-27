package metadata

import (
	"database/sql"
	"fmt"
	"github.com/go-jet/jet/v2/internal/utils"
	"strings"
)

// ColumnMetaData struct
type ColumnMetaData struct {
	Name       string
	IsNullable bool
	DataType   string
	EnumName   string
	IsUnsigned bool

	SqlBuilderColumnType string
	GoBaseType           string
	GoModelType          string
}

// NewColumnMetaData create new column meta data that describes one column in SQL database
func NewColumnMetaData(name string, isNullable bool, dataType string, enumName string, isUnsigned bool) ColumnMetaData {
	columnMetaData := ColumnMetaData{
		Name:       name,
		IsNullable: isNullable,
		DataType:   dataType,
		EnumName:   enumName,
		IsUnsigned: isUnsigned,
	}

	columnMetaData.SqlBuilderColumnType = columnMetaData.getSqlBuilderColumnType()
	columnMetaData.GoBaseType = columnMetaData.getGoBaseType()
	columnMetaData.GoModelType = columnMetaData.getGoModelType()

	return columnMetaData
}

// getSqlBuilderColumnType returns type of jet sql builder column
func (c ColumnMetaData) getSqlBuilderColumnType() string {
	switch c.DataType {
	case "boolean":
		return "Bool"
	case "smallint", "integer", "bigint",
		"tinyint", "mediumint", "int", "year": //MySQL
		return "Integer"
	case "date":
		return "Date"
	case "timestamp without time zone",
		"timestamp", "datetime": //MySQL:
		return "Timestamp"
	case "timestamp with time zone":
		return "Timestampz"
	case "time without time zone",
		"time": //MySQL
		return "Time"
	case "time with time zone":
		return "Timez"
	case "interval":
		return "Interval"
	case "USER-DEFINED", "enum", "text", "character", "character varying", "bytea", "uuid",
		"tsvector", "bit", "bit varying", "money", "json", "jsonb", "xml", "point", "line", "ARRAY",
		"char", "varchar", "binary", "varbinary",
		"tinyblob", "blob", "mediumblob", "longblob", "tinytext", "mediumtext", "longtext": // MySQL
		return "String"
	case "real", "numeric", "decimal", "double precision", "float",
		"double": // MySQL
		return "Float"
	default:
		fmt.Println("- [SQL Builder] Unsupported sql column '" + c.Name + " " + c.DataType + "', using StringColumn instead.")
		return "String"
	}
}

// getGoBaseType returns model type for column info.
func (c ColumnMetaData) getGoBaseType() string {
	switch c.DataType {
	case "USER-DEFINED", "enum":
		return utils.ToGoIdentifier(c.EnumName)
	case "boolean":
		return "bool"
	case "tinyint":
		return "int8"
	case "smallint",
		"year":
		return "int16"
	case "integer",
		"mediumint", "int": //MySQL
		return "int32"
	case "bigint":
		return "int64"
	case "date", "timestamp without time zone", "timestamp with time zone", "time with time zone", "time without time zone",
		"timestamp", "datetime", "time": // MySQL
		return "time.Time"
	case "bytea",
		"binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob": //MySQL
		return "[]byte"
	case "text", "character", "character varying", "tsvector", "bit", "bit varying", "money", "json", "jsonb",
		"xml", "point", "interval", "line", "ARRAY",
		"char", "varchar", "tinytext", "mediumtext", "longtext": // MySQL
		return "string"
	case "real":
		return "float32"
	case "numeric", "decimal", "double precision", "float",
		"double": // MySQL
		return "float64"
	case "uuid":
		return "uuid.UUID"
	default:
		fmt.Println("- [Model      ] Unsupported sql column '" + c.Name + " " + c.DataType + "', using string instead.")
		return "string"
	}
}

// GoModelType returns model type for column info with optional pointer if
// column can be NULL.
func (c ColumnMetaData) getGoModelType() string {
	typeStr := c.GoBaseType

	if strings.Contains(typeStr, "int") && c.IsUnsigned {
		typeStr = "u" + typeStr
	}

	if c.IsNullable {
		return "*" + typeStr
	}

	return typeStr
}

// GoModelTag returns model field tag for column
func (c ColumnMetaData) GoModelTag(isPrimaryKey bool) string {
	tags := []string{}

	if isPrimaryKey {
		tags = append(tags, "primary_key")
	}

	if len(tags) > 0 {
		return "`sql:\"" + strings.Join(tags, ",") + "\"`"
	}

	return ""
}

func getColumnsMetaData(db *sql.DB, querySet DialectQuerySet, schemaName, tableName string) []ColumnMetaData {

	rows, err := db.Query(querySet.ListOfColumnsQuery(), schemaName, tableName)
	utils.PanicOnError(err)
	defer rows.Close()

	ret := []ColumnMetaData{}

	for rows.Next() {
		var name, isNullable, dataType, enumName string
		var isUnsigned bool
		err := rows.Scan(&name, &isNullable, &dataType, &enumName, &isUnsigned)
		utils.PanicOnError(err)

		ret = append(ret, NewColumnMetaData(name, isNullable == "YES", dataType, enumName, isUnsigned))
	}

	err = rows.Err()
	utils.PanicOnError(err)

	return ret
}
