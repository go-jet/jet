package template

import (
	"fmt"
	"github.com/go-jet/jet/v2/generator/metadata"
	"github.com/go-jet/jet/v2/internal/utils"
	"path"
	"unicode"
)

// SQLBuilder is template for generating sql builder files
type SQLBuilder struct {
	Skip  bool
	Path  string
	Table func(table metadata.Table) TableSQLBuilder
	View  func(view metadata.Table) TableSQLBuilder
	Enum  func(enum metadata.Enum) EnumSQLBuilder
}

// DefaultSQLBuilder returns default SQLBuilder implementation
func DefaultSQLBuilder() SQLBuilder {
	return SQLBuilder{
		Path:  "",
		Table: DefaultTableSQLBuilder,
		View:  DefaultViewSQLBuilder,
		Enum:  DefaultEnumSQLBuilder,
	}
}

// UsePath returns new SQLBuilder with new relative path set
func (sb SQLBuilder) UsePath(path string) SQLBuilder {
	sb.Path = path
	return sb
}

// UseTable returns new SQLBuilder with new TableSQLBuilder template function set
func (sb SQLBuilder) UseTable(tableFunc func(table metadata.Table) TableSQLBuilder) SQLBuilder {
	sb.Table = tableFunc
	return sb
}

// UseView returns new SQLBuilder with new ViewSQLBuilder template function set
func (sb SQLBuilder) UseView(viewFunc func(table metadata.Table) ViewSQLBuilder) SQLBuilder {
	sb.View = viewFunc
	return sb
}

// UseEnum returns new SQLBuilder with new EnumSQLBuilder template function set
func (sb SQLBuilder) UseEnum(enumFunc func(enum metadata.Enum) EnumSQLBuilder) SQLBuilder {
	sb.Enum = enumFunc
	return sb
}

// TableSQLBuilder is template for generating table SQLBuilder files
type TableSQLBuilder struct {
	Skip         bool
	Path         string
	FileName     string
	InstanceName string
	TypeName     string
	Column       func(columnMetaData metadata.Column) TableSQLBuilderColumn
}

// ViewSQLBuilder is template for generating view SQLBuilder files
type ViewSQLBuilder = TableSQLBuilder

// DefaultTableSQLBuilder returns default implementation for TableSQLBuilder
func DefaultTableSQLBuilder(tableMetaData metadata.Table) TableSQLBuilder {
	return TableSQLBuilder{
		Path:         "/table",
		FileName:     utils.ToGoFileName(tableMetaData.Name),
		InstanceName: utils.ToGoIdentifier(tableMetaData.Name),
		TypeName:     utils.ToGoIdentifier(tableMetaData.Name) + "Table",
		Column:       DefaultTableSQLBuilderColumn,
	}
}

// DefaultViewSQLBuilder returns default implementation for ViewSQLBuilder
func DefaultViewSQLBuilder(viewMetaData metadata.Table) ViewSQLBuilder {
	tableSQLBuilder := DefaultTableSQLBuilder(viewMetaData)
	tableSQLBuilder.Path = "/view"
	return tableSQLBuilder
}

// PackageName returns package name of table sql builder types
func (tb TableSQLBuilder) PackageName() string {
	return path.Base(tb.Path)
}

// UsePath returns new TableSQLBuilder with new relative path set
func (tb TableSQLBuilder) UsePath(path string) TableSQLBuilder {
	tb.Path = path
	return tb
}

// UseFileName returns new TableSQLBuilder with new file name set
func (tb TableSQLBuilder) UseFileName(name string) TableSQLBuilder {
	tb.FileName = name
	return tb
}

// UseInstanceName returns new TableSQLBuilder with new instance name set
func (tb TableSQLBuilder) UseInstanceName(name string) TableSQLBuilder {
	tb.InstanceName = name
	return tb
}

// UseTypeName returns new TableSQLBuilder with new type name set
func (tb TableSQLBuilder) UseTypeName(name string) TableSQLBuilder {
	tb.TypeName = name
	return tb
}

// UseColumn returns new TableSQLBuilder with new column template function set
func (tb TableSQLBuilder) UseColumn(columnsFunc func(column metadata.Column) TableSQLBuilderColumn) TableSQLBuilder {
	tb.Column = columnsFunc
	return tb
}

// TableSQLBuilderColumn is template for table sql builder column
type TableSQLBuilderColumn struct {
	Name string
	Type string
}

// DefaultTableSQLBuilderColumn returns default implementation of TableSQLBuilderColumn
func DefaultTableSQLBuilderColumn(columnMetaData metadata.Column) TableSQLBuilderColumn {
	return TableSQLBuilderColumn{
		Name: utils.ToGoIdentifier(columnMetaData.Name),
		Type: getSqlBuilderColumnType(columnMetaData),
	}
}

// getSqlBuilderColumnType returns type of jet sql builder column
func getSqlBuilderColumnType(columnMetaData metadata.Column) string {
	if columnMetaData.DataType.Kind != metadata.BaseType {
		return "String"
	}

	switch columnMetaData.DataType.Name {
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
		fmt.Println("- [SQL Builder] Unsupported sql column '" + columnMetaData.Name + " " + columnMetaData.DataType.Name + "', using StringColumn instead.")
		return "String"
	}
}

// EnumSQLBuilder is template for generating enum SQLBuilder files
type EnumSQLBuilder struct {
	Skip         bool
	Path         string
	FileName     string
	InstanceName string
	ValueName    func(enumValue string) string
}

// DefaultEnumSQLBuilder returns default implementation of EnumSQLBuilder
func DefaultEnumSQLBuilder(enumMetaData metadata.Enum) EnumSQLBuilder {
	return EnumSQLBuilder{
		Path:         "/enum",
		FileName:     utils.ToGoFileName(enumMetaData.Name),
		InstanceName: utils.ToGoIdentifier(enumMetaData.Name),
		ValueName: func(enumValue string) string {
			return defaultEnumValueName(enumMetaData.Name, enumValue)
		},
	}
}

// PackageName returns enum sql builder package name
func (e EnumSQLBuilder) PackageName() string {
	return path.Base(e.Path)
}

// UsePath returns new EnumSQLBuilder with new path set
func (e EnumSQLBuilder) UsePath(path string) EnumSQLBuilder {
	e.Path = path
	return e
}

// UseFileName returns new EnumSQLBuilder with new file name set
func (e EnumSQLBuilder) UseFileName(name string) EnumSQLBuilder {
	e.FileName = name
	return e
}

// UseInstanceName returns new EnumSQLBuilder with instance name set
func (e EnumSQLBuilder) UseInstanceName(name string) EnumSQLBuilder {
	e.InstanceName = name
	return e
}

func defaultEnumValueName(enumName, enumValue string) string {
	enumValueName := utils.ToGoIdentifier(enumValue)
	if !unicode.IsLetter([]rune(enumValueName)[0]) {
		return utils.ToGoIdentifier(enumName) + enumValueName
	}

	return enumValueName
}
