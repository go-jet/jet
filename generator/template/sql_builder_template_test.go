package template

import (
	"github.com/go-jet/jet/v2/generator/metadata"
	jet "github.com/go-jet/jet/v2/internal/jet"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestToGoEnumValueIdentifier(t *testing.T) {
	require.Equal(t, defaultEnumValueName("enum_name", "enum_value"), "EnumValue")
	require.Equal(t, defaultEnumValueName("NumEnum", "100"), "NumEnum100")
}

func TestCubridColumnTypes(t *testing.T) {
	tests := []struct {
		dataType string
		want     string
	}{
		{"short", "Integer"},
		{"monetary", "Float"},
		{"string", "String"},
		{"nchar", "String"},
		{"nchar varying", "String"},
		{"clob", "String"},
		{"set", "String"},
		{"multiset", "String"},
		{"sequence", "String"},
		{"list", "String"},
		{"object", "String"},
		{"oid", "String"},
		{"timestamptz", "Timestampz"},
		{"timestampltz", "Timestamp"},
		{"datetimetz", "Timestampz"},
		{"datetimeltz", "Timestamp"},
	}

	for _, tt := range tests {
		t.Run(tt.dataType, func(t *testing.T) {
			col := metadata.Column{
				Name:     "test_col",
				DataType: metadata.DataType{Name: tt.dataType, Kind: metadata.BaseType},
			}
			got := getSqlBuilderColumnType(col)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestColumnRenameReserved(t *testing.T) {
	tests := []struct {
		col  string
		want string
	}{
		{col: "TableName", want: "TableName_"},
		{col: "Table", want: "Table_"},
		{col: "SchemaName", want: "SchemaName_"},
		{col: "Alias", want: "Alias_"},
		{col: "AllColumns", want: "AllColumns_"},
		{col: "MutableColumns", want: "MutableColumns_"},
		{col: "DefaultColumns", want: "DefaultColumns_"},
		{col: "OtherColumn", want: "OtherColumn"},
	}

	for _, tt := range tests {
		t.Run(tt.col, func(t *testing.T) {
			builder := DefaultTableSQLBuilderColumn(metadata.Column{
				Name: tt.col,
			})
			require.Equal(t, builder.Name, tt.want)
		})
	}
}

func TestInsertedRowAlias(t *testing.T) {
	cubridDialect := jet.NewDialect(jet.DialectParams{
		Name:        "CUBRID",
		PackageName: "cubrid",
		ArgumentPlaceholder: func(int) string { return "?" },
		ValuesDefaultColumnName: func(index int) string { return "column_0" },
		JsonValueEncode: func(expr jet.Expression) jet.Expression { return expr },
	})
	mysqlDialect := jet.NewDialect(jet.DialectParams{
		Name:        "MySQL",
		PackageName: "mysql",
		ArgumentPlaceholder: func(int) string { return "?" },
		ValuesDefaultColumnName: func(index int) string { return "column_0" },
		JsonValueEncode: func(expr jet.Expression) jet.Expression { return expr },
	})
	postgresDialect := jet.NewDialect(jet.DialectParams{
		Name:        "PostgreSQL",
		PackageName: "postgres",
		ArgumentPlaceholder: func(ord int) string { return "$1" },
		ValuesDefaultColumnName: func(index int) string { return "column_0" },
		JsonValueEncode: func(expr jet.Expression) jet.Expression { return expr },
	})

	require.Equal(t, "new", insertedRowAlias(cubridDialect))
	require.Equal(t, "new", insertedRowAlias(mysqlDialect))
	require.Equal(t, "excluded", insertedRowAlias(postgresDialect))
}
