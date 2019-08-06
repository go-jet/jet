package metadata

import (
	"database/sql"
	"fmt"
	"github.com/go-jet/jet/internal/utils"
	"strings"
)

// ColumnInfo metadata struct
type ColumnInfo struct {
	Name       string
	IsNullable bool
	DataType   string
	IsUnsigned bool
	EnumName   string
}

// SqlBuilderColumnType returns type of jet sql builder column
func (c ColumnInfo) SqlBuilderColumnType() string {
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
	case "USER-DEFINED", "enum", "text", "character", "character varying", "bytea", "uuid",
		"tsvector", "bit", "bit varying", "money", "json", "jsonb", "xml", "point", "interval", "line", "ARRAY",
		"char", "varchar", "binary", "varbinary",
		"tinyblob", "blob", "mediumblob", "longblob", "tinytext", "mediumtext", "longtext": // MySQL
		return "String"
	case "real", "numeric", "decimal", "double precision", "float",
		"double": // MySQL
		return "Float"
	default:
		fmt.Println("Unsupported sql type: " + c.DataType + ", using string column instead for sql builder.")
		return "String"
	}
}

// GoBaseType returns model type for column info.
func (c ColumnInfo) GoBaseType() string {
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
		fmt.Println("Unsupported sql type: " + c.DataType + ", " + c.EnumName + ", using string instead for model type.")
		return "string"
	}
}

// GoModelType returns model type for column info with optional pointer if
// column can be NULL.
func (c ColumnInfo) GoModelType() string {
	typeStr := c.GoBaseType()

	if strings.Contains(typeStr, "int") && c.IsUnsigned {
		typeStr = "u" + typeStr
	}

	if c.IsNullable {
		return "*" + typeStr
	}

	return typeStr
}

// GoModelTag returns model field tag for column
func (c ColumnInfo) GoModelTag(isPrimaryKey bool) string {
	tags := []string{}

	if isPrimaryKey {
		tags = append(tags, "primary_key")
	}

	if len(tags) > 0 {
		return "`sql:\"" + strings.Join(tags, ",") + "\"`"
	}

	return ""
}

func getColumnInfos(db *sql.DB, querySet MetaDataQuerySet, schemaName, tableName string) ([]ColumnInfo, error) {

	rows, err := db.Query(querySet.ListOfColumnsQuery(), schemaName, tableName)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	ret := []ColumnInfo{}

	for rows.Next() {
		columnInfo := ColumnInfo{}
		var isNullable string
		err := rows.Scan(&columnInfo.Name, &isNullable, &columnInfo.DataType, &columnInfo.EnumName, &columnInfo.IsUnsigned)

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
