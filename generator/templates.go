package generator

var SqlBuilderTableTemplate = `package table

import "github.com/sub0Zero/go-sqlbuilder/sqlbuilder"

type {{.ToGoStructName}} struct {
	sqlbuilder.Table
	
	//Columns
{{- range .Columns}}
	{{.ToGoFieldName}} sqlbuilder.NonAliasColumn
{{- end}}
}

var {{.ToGoVarName}} = &{{.ToGoStructName}}{
	Table: *sqlbuilder.NewTable("{{.Name}}", {{.ToGoColumnFieldList ", "}}),
	
	//Columns
{{- range .Columns}}
	{{.ToGoFieldName}}: {{.ToGoVarName}},
{{- end}}
}

var (
{{- range .Columns}}
	{{.ToGoVarName}} = sqlbuilder.IntColumn("{{.Name}}", {{if .IsNullable}}sqlbuilder.Nullable{{else}}sqlbuilder.NotNullable{{end}})
{{- end}}
)
`

var DataModelTemplate = `package model

type {{.ToGoModelStructName}} struct {
{{- range .Columns}}
	{{.ToGoDMFieldName}} {{.ToGoType}} {{if .IsUnique}}` + "`sql:\"unique\"`" + ` {{end}}
{{- end}}
}
`
