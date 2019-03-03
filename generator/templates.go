package generator

var TableTemplate = `package table

import "github.com/sub0Zero/go-sqlbuilder/sqlbuilder"

type {{camelize .TableInfo.Name}}Table struct {
	sqlbuilder.Table
	
	//Columns
{{- range .TableInfo.Columns}}
	{{camelize .Name}} sqlbuilder.NonAliasColumn
{{- end}}
}

var {{camelize .TableInfo.Name}} = &{{camelize .TableInfo.Name}}Table{
	Table: *sqlbuilder.NewTable("{{.TableInfo.Name}}", {{.ColumnNameList ", "}}),
	
	//Columns
{{- range .TableInfo.Columns}}
	{{camelize .Name}}: {{columnName $.TableInfo.Name .Name}},
{{- end}}
}

var (
{{- range .TableInfo.Columns}}
	{{columnName $.TableInfo.Name .Name}} = sqlbuilder.IntColumn("{{.Name}}", {{if .IsNullable}}sqlbuilder.Nullable{{else}}sqlbuilder.NotNullable{{end}})
{{- end}}
)
`
