package postgres_metadata

import (
	"database/sql"
	"fmt"
	"github.com/go-jet/jet/generator/metadata"
)

type EnumInfo struct {
	name   string
	Values []string
}

func (e EnumInfo) Name() string {
	return e.name
}

func getEnumInfos(db *sql.DB, schemaName string) ([]metadata.MetaData, error) {
	query := `
SELECT t.typname,  
       e.enumlabel
FROM pg_catalog.pg_type t 
   JOIN pg_catalog.pg_enum e on t.oid = e.enumtypid  
   JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
WHERE n.nspname = $1
ORDER BY n.nspname, t.typname, e.enumsortorder;`

	rows, err := db.Query(query, schemaName)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	enumsInfosMap := map[string][]string{}
	for rows.Next() {
		var enumName string
		var enumValue string
		err = rows.Scan(&enumName, &enumValue)
		if err != nil {
			return nil, err
		}

		enumValues := enumsInfosMap[enumName]

		enumValues = append(enumValues, enumValue)

		enumsInfosMap[enumName] = enumValues
	}

	err = rows.Err()

	if err != nil {
		return nil, err
	}

	ret := []metadata.MetaData{}

	for enumName, enumValues := range enumsInfosMap {
		ret = append(ret, EnumInfo{
			enumName,
			enumValues,
		})
	}

	fmt.Println("FOUND", len(ret), " enums")

	return ret, nil
}
