package metadata

import (
	"database/sql"
	"fmt"
	"github.com/serenize/snaker"
)

type ColumnInfo struct {
	Name       string
	IsNullable bool
	DataType   string
	EnumName   string
	TableInfo  *TableInfo
}

func (c ColumnInfo) IsUnique() bool {
	for _, uniqueColumn := range c.TableInfo.PrimaryKeys {
		if uniqueColumn == c.Name {
			return true
		}
	}

	return false
}

func (c ColumnInfo) ToGoVarName() string {
	return snaker.SnakeToCamel(c.Name) + "Column"
}

func (c ColumnInfo) ToSqlBuilderColumnType() string {
	switch c.DataType {
	case "boolean":
		return "BoolColumn"
	case "smallint":
		return "IntegerColumn"
	case "integer":
		return "IntegerColumn"
	case "bigint":
		return "IntegerColumn"
	case "date", "timestamp without time zone", "timestamp with time zone":
		return "TimeColumn"
	case "text", "character", "character varying", "bytea", "uuid":
		return "StringColumn"
	case "real":
		return "NumericColumn"
	case "numeric", "double precision":
		return "NumericColumn"
	default:
		fmt.Println("Unknownl type: " + c.DataType + ", using string column instead.")
		return "StringColumn"
	}
}

func (c ColumnInfo) ToGoType() string {
	typeStr := c.GoBaseType()
	if c.IsNullable || c.TableInfo.IsForeignKey(c.Name) {
		return "*" + typeStr
	}

	return typeStr
}

func (c ColumnInfo) GoBaseType() string {
	if forignKeyTable, ok := c.TableInfo.ForeignTableMap[c.Name]; ok {
		return snaker.SnakeToCamel(forignKeyTable)
	} else {
		switch c.DataType {
		case "USER-DEFINED":
			return snaker.SnakeToCamel(c.EnumName)
		case "boolean":
			return "bool"
		case "smallint":
			return "int16"
		case "integer":
			return "int32"
		case "bigint":
			return "int64"
		case "date", "timestamp without time zone", "timestamp with time zone":
			return "time.Time"
		case "bytea":
			return "[]byte"
		case "text", "character", "character varying":
			return "string"
		case "real":
			return "float32"
		case "numeric", "double precision":
			return "float64"
		case "uuid":
			return "uuid.UUID"
		case "json", "jsonb":
			return "types.JSONText"
		default:
			fmt.Println("Unknown go map type: " + c.DataType + ", " + c.EnumName + ", using string instead.")
			return "string"
		}
	}
}

func (c ColumnInfo) ToGoDMFieldName() string {
	if forignKeyTable, ok := c.TableInfo.ForeignTableMap[c.Name]; ok {
		return snaker.SnakeToCamel(forignKeyTable)
	} else {
		return snaker.SnakeToCamel(c.Name)
	}
}

func (c ColumnInfo) ToGoFieldName() string {
	return snaker.SnakeToCamel(c.Name)
}

func fetchColumnInfos(db *sql.DB, tableInfo *TableInfo) ([]ColumnInfo, error) {

	query := `
SELECT column_name, is_nullable, data_type, udt_name
FROM information_schema.columns
where table_schema = $1 and table_name = $2
order by ordinal_position;`

	//fmt.Println(query)

	rows, err := db.Query(query, tableInfo.DatabaseInfo.SchemaName, &tableInfo.Name)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	ret := []ColumnInfo{}

	for rows.Next() {
		columnInfo := ColumnInfo{}
		var isNullable string
		err := rows.Scan(&columnInfo.Name, &isNullable, &columnInfo.DataType, &columnInfo.EnumName)

		columnInfo.IsNullable = isNullable == "YES"

		if err != nil {
			return nil, err
		}

		columnInfo.TableInfo = tableInfo

		ret = append(ret, columnInfo)
	}

	err = rows.Err()

	if err != nil {
		return nil, err
	}

	return ret, nil
}
