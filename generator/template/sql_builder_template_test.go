package template

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jet/jet/v2/generator/metadata"
)

func TestToGoEnumValueIdentifier(t *testing.T) {
	require.Equal(t, defaultEnumValueName("enum_name", "enum_value"), "EnumValue")
	require.Equal(t, defaultEnumValueName("NumEnum", "100"), "NumEnum100")
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

func Test_SQLBuilder_ShouldSkip(t *testing.T) {
	tests := []struct {
		name    string
		initial SQLBuilder
		skip    bool
	}{
		{
			name:    "True",
			initial: SQLBuilder{Skip: false},
			skip:    true,
		},
		{
			name:    "False",
			initial: SQLBuilder{Skip: true},
			skip:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			updatedBuilder := tt.initial.ShouldSkip(tt.skip)
			require.Equal(t, tt.skip, updatedBuilder.Skip)
		})
	}
}
