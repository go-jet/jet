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

func TestTableModelFieldSourceDialect(t *testing.T) {
	testCases := []struct {
		name          string
		dataTypeName  string
		sourceDialect string
		isNullable    bool
		expectedType  string
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
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			field := DefaultTableModelField(metadata.Column{
				Name:       "field",
				IsNullable: testCase.isNullable,
				DataType: metadata.DataType{
					Name:          testCase.dataTypeName,
					Kind:          metadata.BaseType,
					SourceDialect: testCase.sourceDialect,
				},
			})

			require.Equal(t, testCase.expectedType, field.Type.Name)
		})
	}
}
