package sqlite

import (
	"database/sql"
	"fmt"
	"os"

	"github.com/go-jet/jet/v2/generator/metadata"
	"github.com/go-jet/jet/v2/generator/template"
	"github.com/go-jet/jet/v2/internal/utils"
	"github.com/go-jet/jet/v2/internal/utils/throw"
	"github.com/go-jet/jet/v2/sqlite"
)

// GenerateDSN generates jet files using dsn connection string
func GenerateDSN(dsn, destDir string, templates ...template.Template) error {
	return generate(dsn, "", destDir, templates...)
}

func GenerateFromSchema(dsn, fromSchemaFile, destDir string, templates ...template.Template) error {
	if dsn == "" {
		dsn = ":memory:"
	}
	return generate(dsn, fromSchemaFile, destDir, templates...)
}

func generate(dsn, fromSchemaFile, destDir string, templates ...template.Template) (err error) {
	defer utils.ErrorCatch(&err)

	db, err := sql.Open("sqlite3", dsn)
	throw.OnError(err)
	defer utils.DBClose(db)

	if fromSchemaFile != "" {
		fmt.Println("Writing schema information...")
		schema, err := os.ReadFile(fromSchemaFile)
		throw.OnError(err)
		_, err = db.Exec(string(schema))
		throw.OnError(err)
	}

	fmt.Println("Retrieving schema information...")

	generatorTemplate := template.Default(sqlite.Dialect)
	if len(templates) > 0 {
		generatorTemplate = templates[0]
	}

	schemaMetadata := metadata.GetSchema(db, &sqliteQuerySet{}, "")

	template.ProcessSchema(destDir, schemaMetadata, generatorTemplate)
	return
}
