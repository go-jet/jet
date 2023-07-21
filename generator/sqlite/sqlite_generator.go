package sqlite

import (
	"database/sql"
	"fmt"
	"github.com/go-jet/jet/v2/generator/metadata"
	"github.com/go-jet/jet/v2/generator/template"
	"github.com/go-jet/jet/v2/sqlite"
)

// GenerateDSN generates jet files using dsn connection string
func GenerateDSN(dsn, destDir string, templates ...template.Template) error {
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return fmt.Errorf("failed to open sqlite connection: %w", err)
	}
	defer db.Close()

	fmt.Println("Retrieving schema information...")

	generatorTemplate := template.Default(sqlite.Dialect)
	if len(templates) > 0 {
		generatorTemplate = templates[0]
	}

	schemaMetadata, err := metadata.GetSchema(db, &sqliteQuerySet{}, "")
	if err != nil {
		return fmt.Errorf("failed to query database metadata: %w", err)
	}

	err = template.ProcessSchema(destDir, schemaMetadata, generatorTemplate)
	if err != nil {
		return fmt.Errorf("failed to process database %s: %w", schemaMetadata.Name, err)
	}

	return nil
}
