package cubrid

import (
	"testing"

	"github.com/go-jet/jet/v2/generator/metadata"
)

func TestMapCubridDataType(t *testing.T) {
	tests := []struct {
		cubridType string
		want       string
	}{
		{"BIT", "boolean"},
		{"SHORT", "smallint"}, {"SMALLINT", "smallint"},
		{"INT", "integer"}, {"INTEGER", "integer"},
		{"BIGINT", "bigint"},
		{"FLOAT", "real"}, {"REAL", "real"},
		{"DOUBLE", "double"}, {"DOUBLE PRECISION", "double"},
		{"NUMERIC", "numeric"}, {"DECIMAL", "numeric"}, {"MONETARY", "numeric"},
		{"CHAR", "char"}, {"VARCHAR", "varchar"}, {"STRING", "varchar"},
		{"NCHAR", "char"}, {"NCHAR VARYING", "varchar"},
		{"ENUM", "enum"}, {"JSON", "json"},
		{"DATE", "date"}, {"TIME", "time"},
		{"DATETIME", "datetime"}, {"TIMESTAMP", "timestamp"},
		{"TIMESTAMPTZ", "timestamp"}, {"DATETIMETZ", "datetime"},
		{"BIT VARYING", "blob"}, {"BLOB", "blob"}, {"CLOB", "text"},
		{"SET", "json"}, {"MULTISET", "json"}, {"SEQUENCE", "json"},
		{"OBJECT", "varchar"}, {"OID", "varchar"},
		{"int", "integer"}, {"Varchar", "varchar"},
		{"DEC", "numeric"}, {"CHARACTER", "char"}, {"CHARACTER VARYING", "varchar"},
		{"UTIME", "timestamp"}, {"TIMESTAMPLTZ", "timestamp"}, {"DATETIMELTZ", "datetime"},
		{"VARBIT", "blob"}, {"LIST", "json"},
		{"UNKNOWN_CUSTOM_TYPE", "unknown_custom_type"}, // default branch
	}
	for _, tt := range tests {
		t.Run(tt.cubridType, func(t *testing.T) {
			if got := mapCubridDataType(tt.cubridType); got != tt.want {
				t.Errorf("mapCubridDataType(%q) = %q, want %q", tt.cubridType, got, tt.want)
			}
		})
	}
}

func TestDataTypeKind(t *testing.T) {
	tests := []struct {
		in   string
		want metadata.DataTypeKind
	}{
		{"ENUM", metadata.EnumType},
		{"INT", metadata.BaseType},
		{"VARCHAR", metadata.BaseType},
	}
	for _, tt := range tests {
		if got := dataTypeKind(tt.in); got != tt.want {
			t.Errorf("dataTypeKind(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}
