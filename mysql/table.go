package mysql

import "github.com/go-jet/jet/internal/jet"

type Table jet.Table

func NewTable(schemaName, name string, columns ...jet.Column) Table {
	return jet.NewTable(Dialect, schemaName, name, columns...)
}
