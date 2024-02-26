package template

import (
	"fmt"
	"github.com/go-jet/jet/v2/generator/metadata"
	"github.com/go-jet/jet/v2/internal/utils/dbidentifier"
	"github.com/google/uuid"
	"github.com/jackc/pgtype"
	"path"
	"reflect"
	"strings"
	"time"
)

// Model is template for model files generation
type Model struct {
	Skip  bool
	Path  string
	Table func(table metadata.Table) TableModel
	View  func(table metadata.Table) ViewModel
	Enum  func(enum metadata.Enum) EnumModel
}

// PackageName returns package name of model types
func (m Model) PackageName() string {
	return path.Base(m.Path)
}

// UsePath returns new Model template with replaced file path
func (m Model) UsePath(path string) Model {
	m.Path = path
	return m
}

// UseTable returns new Model template with replaced template for table model files generation
func (m Model) UseTable(tableModelFunc func(table metadata.Table) TableModel) Model {
	m.Table = tableModelFunc
	return m
}

// UseView returns new Model template with replaced template for view model files generation
func (m Model) UseView(tableModelFunc func(table metadata.Table) TableModel) Model {
	m.View = tableModelFunc
	return m
}

// UseEnum returns new Model template with replaced template for enum model files generation
func (m Model) UseEnum(enumFunc func(enumMetaData metadata.Enum) EnumModel) Model {
	m.Enum = enumFunc
	return m
}

// DefaultModel returns default Model template implementation
func DefaultModel() Model {
	return Model{
		Skip:  false,
		Path:  "/model",
		Table: DefaultTableModel,
		View:  DefaultViewModel,
		Enum:  DefaultEnumModel,
	}
}

// TableModel is template for table model files generation
type TableModel struct {
	Skip     bool
	FileName string
	TypeName string
	Field    func(columnMetaData metadata.Column) TableModelField
}

// ViewModel is template for view model files generation
type ViewModel = TableModel

// DefaultViewModel is default view template implementation
var DefaultViewModel = DefaultTableModel

// DefaultTableModel is default table template implementation
func DefaultTableModel(tableMetaData metadata.Table) TableModel {
	return TableModel{
		FileName: dbidentifier.ToGoFileName(tableMetaData.Name),
		TypeName: dbidentifier.ToGoIdentifier(tableMetaData.Name),
		Field:    DefaultTableModelField,
	}
}

// UseFileName returns new TableModel with new file name set
func (t TableModel) UseFileName(fileName string) TableModel {
	t.FileName = fileName
	return t
}

// UseTypeName returns new TableModel with new type name set
func (t TableModel) UseTypeName(typeName string) TableModel {
	t.TypeName = typeName
	return t
}

// UseField returns new TableModel with new TableModelField template function
func (t TableModel) UseField(structFieldFunc func(columnMetaData metadata.Column) TableModelField) TableModel {
	t.Field = structFieldFunc
	return t
}

func getTableModelImports(modelType TableModel, tableMetaData metadata.Table) []string {
	importPaths := map[string]bool{}
	for _, columnMetaData := range tableMetaData.Columns {
		field := modelType.Field(columnMetaData)
		importPath := field.Type.ImportPath

		if importPath != "" {
			importPaths[importPath] = true
		}
	}

	var ret []string
	for importPath := range importPaths {
		ret = append(ret, importPath)
	}

	return ret
}

// EnumModel is template for enum model files generation
type EnumModel struct {
	Skip      bool
	FileName  string
	TypeName  string
	ValueName func(value string) string
}

// UseFileName returns new EnumModel with new file name set
func (em EnumModel) UseFileName(fileName string) EnumModel {
	em.FileName = fileName
	return em
}

// UseTypeName returns new EnumModel with new type name set
func (em EnumModel) UseTypeName(typeName string) EnumModel {
	em.TypeName = typeName
	return em
}

// DefaultEnumModel returns default implementation for EnumModel
func DefaultEnumModel(enumMetaData metadata.Enum) EnumModel {
	typeName := dbidentifier.ToGoIdentifier(enumMetaData.Name)

	return EnumModel{
		FileName: dbidentifier.ToGoFileName(enumMetaData.Name),
		TypeName: typeName,
		ValueName: func(value string) string {
			return typeName + "_" + dbidentifier.ToGoIdentifier(value)
		},
	}
}

// TableModelField is template for table model field generation
type TableModelField struct {
	Name string
	Type Type
	Tags []string
}

// DefaultTableModelField returns default TableModelField implementation
func DefaultTableModelField(columnMetaData metadata.Column) TableModelField {
	var tags []string

	if columnMetaData.IsPrimaryKey {
		tags = append(tags, `sql:"primary_key"`)
	}

	return TableModelField{
		Name: dbidentifier.ToGoIdentifier(columnMetaData.Name),
		Type: getType(columnMetaData),
		Tags: tags,
	}
}

// UseType returns new TypeModelField with a new field type set
func (f TableModelField) UseType(t Type) TableModelField {
	f.Type = t
	return f
}

// UseName returns new TableModelField implementation with new field name set
func (f TableModelField) UseName(name string) TableModelField {
	f.Name = name
	return f
}

// UseTags returns new TableModelField implementation with additional tags added.
func (f TableModelField) UseTags(tags ...string) TableModelField {
	f.Tags = append(f.Tags, tags...)
	return f
}

// TagsString returns tags string representation
func (f TableModelField) TagsString() string {
	if len(f.Tags) == 0 {
		return ""
	}

	return fmt.Sprintf("`%s`", strings.Join(f.Tags, " "))
}

// Type represents type of the struct field
type Type struct {
	ImportPath string
	Name       string
}

// NewType creates new type for dummy object
func NewType(dummyObject interface{}) Type {
	return Type{
		ImportPath: getImportPath(dummyObject),
		Name:       getTypeName(dummyObject),
	}
}

func getTypeName(t interface{}) string {
	typeStr := reflect.TypeOf(t).String()
	typeStr = strings.Replace(typeStr, "[]uint8", "[]byte", -1)

	return typeStr
}

func getImportPath(dummyData interface{}) string {
	dataType := reflect.TypeOf(dummyData)
	if dataType.Kind() == reflect.Ptr {
		return dataType.Elem().PkgPath()
	}
	return dataType.PkgPath()
}

func getType(columnMetadata metadata.Column) Type {
	userDefinedType := getUserDefinedType(columnMetadata)

	if userDefinedType != "" {
		if columnMetadata.IsNullable {
			return Type{Name: "*" + userDefinedType}
		}
		return Type{Name: userDefinedType}
	}

	return NewType(getGoType(columnMetadata))
}

func getUserDefinedType(column metadata.Column) string {
	switch column.DataType.Kind {
	case metadata.EnumType:
		return dbidentifier.ToGoIdentifier(column.DataType.Name)
	case metadata.UserDefinedType, metadata.ArrayType:
		return "string"
	}

	return ""
}

func getGoType(column metadata.Column) interface{} {
	defaultGoType := toGoType(column)

	if column.IsNullable {
		return reflect.New(reflect.TypeOf(defaultGoType)).Interface()
	}

	return defaultGoType
}

// toGoType returns model type for column info.
func toGoType(column metadata.Column) interface{} {
	switch strings.ToLower(column.DataType.Name) {
	case "user-defined", "enum":
		return ""
	case "boolean", "bool":
		return false
	case "tinyint":
		if column.DataType.IsUnsigned {
			return uint8(0)
		}
		return int8(0)
	case "smallint", "int2",
		"year":
		if column.DataType.IsUnsigned {
			return uint16(0)
		}
		return int16(0)
	case "integer", "int4",
		"mediumint", "int": //MySQL
		if column.DataType.IsUnsigned {
			return uint32(0)
		}
		return int32(0)
	case "bigint", "int8":
		if column.DataType.IsUnsigned {
			return uint64(0)
		}
		return int64(0)
	case "date",
		"timestamp without time zone", "timestamp",
		"timestamp with time zone", "timestamptz",
		"time without time zone", "time",
		"time with time zone", "timetz",
		"datetime": // MySQL
		return time.Time{}
	case "bytea",
		"binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob": //MySQL
		return []byte("")
	case "text",
		"character", "bpchar",
		"character varying", "varchar", "nvarchar",
		"tsvector", "bit", "bit varying", "varbit",
		"money", "json", "jsonb",
		"xml", "point", "interval", "line", "array",
		"char", "tinytext", "mediumtext", "longtext": // MySQL
		return ""
	case "real", "float4":
		return float32(0.0)
	case "numeric", "decimal",
		"double precision", "float8", "float",
		"double": // MySQL
		return float64(0.0)
	case "uuid":
		return uuid.UUID{}
	case "daterange":
		return pgtype.Daterange{}
	case "tsrange":
		return pgtype.Tsrange{}
	case "tstzrange":
		return pgtype.Tstzrange{}
	case "int4range":
		return pgtype.Int4range{}
	case "int8range":
		return pgtype.Int8range{}
	case "numrange":
		return pgtype.Numrange{}
	default:
		fmt.Println("- [Model      ] Unsupported sql column '" + column.Name + " " + column.DataType.Name + "', using string instead.")
		return ""
	}
}
