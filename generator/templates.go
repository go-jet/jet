package generator

var SqlBuilderTableTemplate = `package table

import (
	"github.com/sub0Zero/go-sqlbuilder/sqlbuilder"
)

type {{.ToGoStructName}} struct {
	sqlbuilder.Table
	
	//Columns
{{- range .Columns}}
	{{.ToGoFieldName}} *sqlbuilder.{{.ToSqlBuilderColumnType}}
{{- end}}

	AllColumns sqlbuilder.ColumnList
}

var {{.ToGoVarName}} = new{{.ToGoStructName}}()

func new{{.ToGoStructName}}() *{{.ToGoStructName}} {
	var (
	{{- range .Columns}}
		{{.ToGoVarName}} = sqlbuilder.New{{.ToSqlBuilderColumnType}}("{{.Name}}", {{if .IsNullable}}sqlbuilder.Nullable{{else}}sqlbuilder.NotNullable{{end}})
	{{- end}}
	)

	return &{{.ToGoStructName}}{
		Table: *sqlbuilder.NewTable("{{.DatabaseInfo.SchemaName}}", "{{.Name}}", {{.ToGoColumnFieldList ", "}}),

		//Columns
{{- range .Columns}}
		{{.ToGoFieldName}}: {{.ToGoVarName}},
{{- end}}

		AllColumns: sqlbuilder.ColumnList{ {{.ToGoColumnFieldList ", "}} },
	}
}


func (a *{{.ToGoStructName}}) As(alias string) *{{.ToGoStructName}} {
	aliasTable := new{{.ToGoStructName}}()

	aliasTable.Table.SetAlias(alias)

	return aliasTable
}

`

var DataModelTemplate = `package model

{{range .GetImports}}
	import "{{.}}"
{{end}}

type {{.ToGoModelStructName}} struct {
{{- range .Columns}}
	{{.ToGoDMFieldName}} {{.ToGoType}} {{if .IsUnique}}` + "`sql:\"unique\"`" + ` {{end}}
{{- end}}
}
`

var EnumModelTemplate = `package model

import "errors"

type {{.Name}} string

const (
{{- range $index, $element := .Values}}
	{{camelize $.Name}}_{{camelize $element}} {{$.Name}} = "{{$element}}"
{{- end}}
)

func (e *{{$.Name}}) Scan(value interface{}) error {
	if v, ok := value.(string); !ok {
		return errors.New("Invalid data for {{$.Name}} enum")
	} else {
		switch string(v) {
{{- range $index, $element := .Values}}
		case "{{$element}}":
			*e = {{camelize $.Name}}_{{camelize $element}}
{{- end}}
		default:
			return errors.New("Inavlid data " + string(v) + "for {{$.Name}} enum")
		}

		return nil
	}
}

`
