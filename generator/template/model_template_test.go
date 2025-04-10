package template

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jet/jet/v2/generator/metadata"
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

func Test_Model_ShouldSkip(t *testing.T) {
	tests := []struct {
		name    string
		initial Model
		skip    bool
	}{
		{
			name:    "True",
			initial: Model{Skip: false},
			skip:    true,
		},
		{
			name:    "False",
			initial: Model{Skip: true},
			skip:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			updatedModel := tt.initial.ShouldSkip(tt.skip)
			require.Equal(t, tt.skip, updatedModel.Skip)
		})
	}
}
