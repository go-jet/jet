package template

import (
	"testing"

	"github.com/go-jet/jet/v2/generator/metadata"
	"github.com/stretchr/testify/require"
)

func TestTableModelField(t *testing.T) {
	require.Equal(t, DefaultTableModelField(metadata.Column{
		Name:         "col_name",
		IsPrimaryKey: true,
		IsNullable:   true,
		DataType: metadata.DataType{
			Name:       "smallint",
			Kind:       "base",
			IsUnsigned: true,
		},
	}), TableModelField{
		Name: "ColName",
		Type: Type{
			ImportPath: "",
			Name:       "*uint16",
		},
		Tags: []string{"sql:\"primary_key\""},
	})

	require.Equal(t, DefaultTableModelField(metadata.Column{
		Name:         "time_column_1",
		IsPrimaryKey: false,
		IsNullable:   true,
		DataType: metadata.DataType{
			Name:       "timestamp with time zone",
			Kind:       "base",
			IsUnsigned: false,
		},
	}), TableModelField{
		Name: "TimeColumn1",
		Type: Type{
			ImportPath: "time",
			Name:       "*time.Time",
		},
		Tags: nil,
	})
}

func TestAddColumnTag(t *testing.T) {
	t.Run("default name", func(t *testing.T) {
		field := TableModelField{Name: "FooID"}
		column := metadata.Column{Name: "foo_id"}
		resultField := addColumnTag(field, column)
		require.NotContains(t, resultField.Tags, `column:"foo_id"`)
	})
	t.Run("custom name", func(t *testing.T) {
		field := TableModelField{Name: "CustomINITIALISM"}
		column := metadata.Column{Name: "custom_initialism"}
		resultField := addColumnTag(field, column)
		require.Contains(t, resultField.Tags, `column:"custom_initialism"`)
	})
	t.Run("custom name and tag", func(t *testing.T) {
		field := TableModelField{
			Name: "CustomINITIALISM",
			Tags: []string{`column:"some_column"`},
		}
		column := metadata.Column{Name: "custom_initialism"}
		resultField := addColumnTag(field, column)
		require.Contains(t, resultField.Tags, `column:"some_column"`)
		require.NotContains(t, resultField.Tags, `column:"custom_initialism"`)
	})
}

func TestTableModelFieldSourceDialect(t *testing.T) {
	testCases := []struct {
		name               string
		dataTypeName       string
		sourceDialect      string
		isNullable         bool
		dimensions         int
		expectedType       string
		expectedImportPath string
	}{
		{
			name:          "sqlite integer",
			dataTypeName:  "INTEGER",
			sourceDialect: "SQLite",
			expectedType:  "int64",
		},
		{
			name:          "sqlite int",
			dataTypeName:  "INT",
			sourceDialect: "SQLite",
			expectedType:  "int64",
		},
		{
			name:          "sqlite nullable integer",
			dataTypeName:  "INTEGER",
			sourceDialect: "SQLite",
			isNullable:    true,
			expectedType:  "*int64",
		},
		{
			name:          "sqlite real",
			dataTypeName:  "REAL",
			sourceDialect: "SQLite",
			expectedType:  "float64",
		},
		{
			name:          "sqlite nullable real",
			dataTypeName:  "REAL",
			sourceDialect: "SQLite",
			isNullable:    true,
			expectedType:  "*float64",
		},
		{
			name:          "postgres integer",
			dataTypeName:  "integer",
			sourceDialect: "PostgreSQL",
			expectedType:  "int32",
		},
		{
			name:          "postgres real",
			dataTypeName:  "real",
			sourceDialect: "PostgreSQL",
			expectedType:  "float32",
		},
		{
			name:          "mysql int",
			dataTypeName:  "int",
			sourceDialect: "MySQL",
			expectedType:  "int32",
		},
		{
			name:          "mysql int",
			dataTypeName:  "int",
			sourceDialect: "MySQL",
			dimensions:    4,
			expectedType:  "string",
		},
		{
			name:               "postgres decimal",
			dataTypeName:       "decimal",
			sourceDialect:      "PostgreSQL",
			expectedType:       "decimal.Decimal",
			expectedImportPath: "github.com/shopspring/decimal",
		},
		{
			name:               "mysql numeric",
			dataTypeName:       "numeric",
			sourceDialect:      "MySQL",
			expectedType:       "decimal.Decimal",
			expectedImportPath: "github.com/shopspring/decimal",
		},
		{
			name:               "sqlite nullable numeric",
			dataTypeName:       "numeric",
			sourceDialect:      "SQLite",
			isNullable:         true,
			expectedType:       "*decimal.Decimal",
			expectedImportPath: "github.com/shopspring/decimal",
		},
		{
			name:               "postgres numeric array",
			dataTypeName:       "numeric",
			sourceDialect:      "PostgreSQL",
			dimensions:         1,
			expectedType:       "pq.StringArray",
			expectedImportPath: "github.com/lib/pq",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			field := DefaultTableModelField(metadata.Column{
				Name:       "field",
				IsNullable: testCase.isNullable,
				DataType: metadata.DataType{
					Name:          testCase.dataTypeName,
					Kind:          metadata.BaseType,
					Dimensions:    testCase.dimensions,
					SourceDialect: testCase.sourceDialect,
				},
			})

			require.Equal(t, testCase.expectedType, field.Type.Name)
			require.Equal(t, testCase.expectedImportPath, field.Type.ImportPath)
		})
	}
}

func TestTableModelFieldPostgresRangeTypes(t *testing.T) {
	testCases := []struct {
		name         string
		dataTypeName string
		isNullable   bool
		expectedType string
	}{
		{
			name:         "date range",
			dataTypeName: "daterange",
			expectedType: "pgtype.Daterange",
		},
		{
			name:         "timestamp range",
			dataTypeName: "tsrange",
			expectedType: "pgtype.Tsrange",
		},
		{
			name:         "nullable numeric range",
			dataTypeName: "numrange",
			isNullable:   true,
			expectedType: "*pgtype.Numrange",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			field := DefaultTableModelField(metadata.Column{
				Name:       "field",
				IsNullable: testCase.isNullable,
				DataType: metadata.DataType{
					Name: testCase.dataTypeName,
					Kind: metadata.BaseType,
				},
			})

			require.Equal(t, testCase.expectedType, field.Type.Name)
			require.Equal(t, "github.com/jackc/pgtype", field.Type.ImportPath)
		})
	}
}
