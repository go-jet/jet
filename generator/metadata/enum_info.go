package metadata

import (
	"database/sql"
	"fmt"
)

type EnumInfo struct {
	Name   string
	Values []string
}

func (e *EnumInfo) goValueName(index int) {
	return
}

func fetchEnumInfos(db *sql.DB, databaseInfo *DatabaseInfo) ([]EnumInfo, error) {
	query := `
SELECT t.typname,  
       e.enumlabel
FROM pg_catalog.pg_type t 
   JOIN pg_catalog.pg_enum e on t.oid = e.enumtypid  
   JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
WHERE n.nspname = $1
ORDER BY n.nspname, t.typname, e.enumsortorder;`

	//fmt.Println(query, schemaName)

	rows, err := db.Query(query, &databaseInfo.SchemaName)

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

	ret := []EnumInfo{}

	for enumName, enumValues := range enumsInfosMap {
		ret = append(ret, EnumInfo{
			enumName,
			enumValues,
		})
	}

	fmt.Println("FOUND", len(ret), " enums")

	return ret, nil
}
