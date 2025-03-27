package template

import (
	"database/sql"
	"fmt"
	"github.com/go-jet/jet/v2/generator/metadata"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"math/big"
	"reflect"
	"strings"
	"testing"
	"time"
)

type Foo[Q, T, P, U, K, Y, G, H any] struct{}

func TestFoo(t *testing.T) {
	var obj any
	obj = Foo[uuid.UUID, *uuid.UUID, **uuid.UUID, big.Int, *big.Int, **big.Int, string, *string]{}
	typ := reflect.TypeOf(obj)
	fmt.Println("Name:", typ.Name())
	fmt.Println("String:", typ.String())
	fmt.Println("PkgPath:", typ.PkgPath())

	importPaths := make(map[string]struct{})
	importPaths[typ.PkgPath()] = struct{}{}

	fullName := typ.String()
	idx := strings.Index(fullName, "[")
	if idx <= 0 {
		fmt.Println("Not generic")
		return
	}
	fmt.Println("Generic")

	var innerTypes []string

	genericTypesString := fullName[idx+1 : len(fullName)-1]
	genericTypes := strings.Split(genericTypesString, ",")
	fmt.Println(genericTypes)
	for _, p := range genericTypes {
		lastSlashIdx := strings.LastIndex(p, "/")
		if lastSlashIdx == -1 {
			innerTypes = append(innerTypes, p)
			continue
		}

		var typePrefix string
		if pointerIdx := strings.LastIndex(p, "*"); pointerIdx != -1 {
			typePrefix = p[:pointerIdx+1]
		}

		innerTypes = append(innerTypes, typePrefix+p[lastSlashIdx+1:])

		lastDot := strings.LastIndex(p, ".")
		importPath := p[:lastDot]
		if typePrefix != "" {
			importPath = strings.TrimPrefix(importPath, typePrefix)
		}
		importPaths[importPath] = struct{}{}
	}

	fmt.Println("Imports:", importPaths)
	//fmt.Println("Types:", innerTypes)

	fmt.Println("Final:", fmt.Sprintf("%s[%s]", strings.Split(typ.String(), "[")[0], strings.Join(innerTypes, ",")))
}

func Test_ParseType(t *testing.T) {
	type GenericSingle[T any] struct{}
	type GenericMultiple[T, U any] struct{}

	for _, tt := range []struct {
		obj             any
		expectedTypeStr string
		expectedImports []string
	}{
		{
			obj:             "",
			expectedTypeStr: "string",
		},
		{
			obj:             new(string),
			expectedTypeStr: "*string",
		},
		{
			obj:             time.Time{},
			expectedTypeStr: "time.Time",
			expectedImports: []string{"time"},
		},
		{
			obj:             sql.NullString{},
			expectedTypeStr: "sql.NullString",
			expectedImports: []string{"database/sql"},
		},
		{
			obj:             &sql.NullString{},
			expectedTypeStr: "*sql.NullString",
			expectedImports: []string{"database/sql"},
		},
		{
			obj:             &sql.NullString{},
			expectedTypeStr: "*sql.NullString",
			expectedImports: []string{"database/sql"},
		},
		{
			obj:             []uint8{},
			expectedTypeStr: "[]byte",
		},
		{
			obj:             &[]uint8{},
			expectedTypeStr: "*[]byte",
		},
		{
			obj:             GenericSingle[int]{},
			expectedTypeStr: "template.GenericSingle[int]",
			expectedImports: []string{"github.com/go-jet/jet/v2/generator/template"},
		},
		{
			obj:             GenericSingle[*int]{},
			expectedTypeStr: "template.GenericSingle[*int]",
			expectedImports: []string{"github.com/go-jet/jet/v2/generator/template"},
		},
		{
			obj:             GenericSingle[[]byte]{},
			expectedTypeStr: "template.GenericSingle[[]byte]",
			expectedImports: []string{"github.com/go-jet/jet/v2/generator/template"},
		},
		{
			obj:             GenericSingle[time.Time]{},
			expectedTypeStr: "template.GenericSingle[time.Time]",
			expectedImports: []string{"github.com/go-jet/jet/v2/generator/template", "time"},
		},
		{
			obj:             GenericSingle[*time.Time]{},
			expectedTypeStr: "template.GenericSingle[*time.Time]",
			expectedImports: []string{"github.com/go-jet/jet/v2/generator/template", "time"},
		},
		{
			obj:             GenericMultiple[*time.Time, sql.NullString]{},
			expectedTypeStr: "template.GenericMultiple[*time.Time,sql.NullString]",
			expectedImports: []string{"github.com/go-jet/jet/v2/generator/template", "time", "database/sql"},
		},
		{
			obj:             GenericMultiple[*sql.NullString, sql.NullString]{},
			expectedTypeStr: "template.GenericMultiple[*sql.NullString,sql.NullString]",
			expectedImports: []string{"github.com/go-jet/jet/v2/generator/template", "database/sql"},
		},
		{
			obj:             &GenericMultiple[*sql.NullString, sql.NullString]{},
			expectedTypeStr: "*template.GenericMultiple[*sql.NullString,sql.NullString]",
			expectedImports: []string{"github.com/go-jet/jet/v2/generator/template", "database/sql"},
		},
		{
			obj:             Foo[uuid.UUID, *uuid.UUID, **uuid.UUID, big.Int, *big.Int, **big.Int, string, *string]{},
			expectedTypeStr: "template.Foo[uuid.UUID,*uuid.UUID,**uuid.UUID,big.Int,*big.Int,**big.Int,string,*string]",
			expectedImports: []string{"github.com/google/uuid", "math/big", "github.com/go-jet/jet/v2/generator/template"},
		},
	} {
		typeStr, imports := parseType(tt.obj)
		require.Equal(t, tt.expectedTypeStr, typeStr)
		require.ElementsMatch(t, tt.expectedImports, imports)
	}
}

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
