package template

import (
	"github.com/go-jet/jet/v2/generator/metadata"
	"github.com/go-jet/jet/v2/internal/jet"
)

// Template is generator template used for file generation
type Template struct {
	Dialect jet.Dialect
	Schema  func(schemaMetaData metadata.Schema) Schema
}

// Default is default generator template implementation
func Default(dialect jet.Dialect) Template {
	return Template{
		Dialect: dialect,
		Schema:  DefaultSchema,
	}
}

// UseSchema replaces current schema generate function with a new implementation and returns new generator template
func (t Template) UseSchema(schemaFunc func(schemaMetaData metadata.Schema) Schema) Template {
	t.Schema = schemaFunc
	return t
}

// Schema is schema generator template used to generate schema(model and sql builder) files
type Schema struct {
	Path       string
	Model      Model
	SQLBuilder SQLBuilder
}

// UsePath replaces path and returns new schema template
func (s Schema) UsePath(path string) Schema {
	s.Path = path
	return s
}

// UseModel returns new schema template with replaced template for model files generation
func (s Schema) UseModel(model Model) Schema {
	s.Model = model
	return s
}

// UseSQLBuilder returns new schema with replaced template for sql builder files generation
func (s Schema) UseSQLBuilder(sqlBuilder SQLBuilder) Schema {
	s.SQLBuilder = sqlBuilder
	return s
}

// DefaultSchema returns default schema template implementation
func DefaultSchema(schemaMetaData metadata.Schema) Schema {
	return Schema{
		Path:       schemaMetaData.Name,
		Model:      DefaultModel(),
		SQLBuilder: DefaultSQLBuilder(),
	}
}
