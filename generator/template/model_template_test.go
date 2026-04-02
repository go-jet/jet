package template

import (
	"github.com/go-jet/jet/v2/generator/metadata"
	"github.com/stretchr/testify/require"
	"testing"
)

func Test_TableModelField(t *testing.T) {
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

	// CUBRID SHORT type
	require.Equal(t, DefaultTableModelField(metadata.Column{
		Name:     "short_col",
		DataType: metadata.DataType{Name: "short", Kind: "base"},
	}), TableModelField{
		Name: "ShortCol",
		Type: Type{Name: "int16"},
	})

	// CUBRID MONETARY type
	require.Equal(t, DefaultTableModelField(metadata.Column{
		Name:     "money_col",
		DataType: metadata.DataType{Name: "monetary", Kind: "base"},
	}), TableModelField{
		Name: "MoneyCol",
		Type: Type{Name: "float64"},
	})

	// CUBRID DATETIMETZ type
	require.Equal(t, DefaultTableModelField(metadata.Column{
		Name:     "dtz_col",
		DataType: metadata.DataType{Name: "datetimetz", Kind: "base"},
	}), TableModelField{
		Name: "DtzCol",
		Type: Type{ImportPath: "time", Name: "time.Time"},
	})

	// CUBRID CLOB type
	require.Equal(t, DefaultTableModelField(metadata.Column{
		Name:     "clob_col",
		DataType: metadata.DataType{Name: "clob", Kind: "base"},
	}), TableModelField{
		Name: "ClobCol",
		Type: Type{Name: "string"},
	})

	// CUBRID SET collection type
	require.Equal(t, DefaultTableModelField(metadata.Column{
		Name:     "set_col",
		DataType: metadata.DataType{Name: "set", Kind: "base"},
	}), TableModelField{
		Name: "SetCol",
		Type: Type{Name: "string"},
	})

	// CUBRID OID type
	require.Equal(t, DefaultTableModelField(metadata.Column{
		Name:     "oid_col",
		DataType: metadata.DataType{Name: "oid", Kind: "base"},
	}), TableModelField{
		Name: "OidCol",
		Type: Type{Name: "string"},
	})

	// CUBRID TIMESTAMPLTZ (nullable)
	require.Equal(t, DefaultTableModelField(metadata.Column{
		Name:       "ts_ltz",
		IsNullable: true,
		DataType:   metadata.DataType{Name: "timestampltz", Kind: "base"},
	}), TableModelField{
		Name: "TsLtz",
		Type: Type{ImportPath: "time", Name: "*time.Time"},
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
