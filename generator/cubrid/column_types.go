package cubrid

import (
	"strings"

	"github.com/go-jet/jet/v2/generator/metadata"
)

// mapCubridDataType maps CUBRID data type names to go-jet compatible type names.
func mapCubridDataType(cubridType string) string {
	switch strings.ToUpper(cubridType) {
	case "BIT":
		return "boolean"
	case "SHORT", "SMALLINT":
		return "smallint"
	case "INT", "INTEGER":
		return "integer"
	case "BIGINT":
		return "bigint"
	case "FLOAT", "REAL":
		return "real"
	case "DOUBLE", "DOUBLE PRECISION":
		return "double"
	case "NUMERIC", "DECIMAL", "DEC", "MONETARY":
		return "numeric"
	case "CHAR", "CHARACTER", "NCHAR":
		return "char"
	case "VARCHAR", "CHARACTER VARYING", "STRING", "NCHAR VARYING":
		return "varchar"
	case "ENUM":
		return "enum"
	case "JSON":
		return "json"
	case "DATE":
		return "date"
	case "TIME":
		return "time"
	case "DATETIME", "DATETIMETZ", "DATETIMELTZ":
		return "datetime"
	case "TIMESTAMP", "UTIME", "TIMESTAMPTZ", "TIMESTAMPLTZ":
		return "timestamp"
	case "BIT VARYING", "VARBIT", "BLOB":
		return "blob"
	case "CLOB":
		return "text"
	case "SET", "MULTISET", "SEQUENCE", "LIST":
		return "json"
	case "OBJECT", "OID":
		return "varchar"
	default:
		return strings.ToLower(cubridType)
	}
}

// dataTypeKind returns the metadata DataTypeKind for a CUBRID data type.
func dataTypeKind(cubridType string) metadata.DataTypeKind {
	if strings.ToUpper(cubridType) == "ENUM" {
		return metadata.EnumType
	}
	return metadata.BaseType
}
