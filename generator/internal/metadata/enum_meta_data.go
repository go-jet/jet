package metadata

import (
	"database/sql"
)

// EnumMetaData struct
type EnumMetaData struct {
	name   string
	Values []string
}

// Name returns enum name
func (e EnumMetaData) Name() string {
	return e.name
}

func getEnumInfos(db *sql.DB, querySet DialectQuerySet, schemaName string) ([]MetaData, error) {

	rows, err := db.Query(querySet.ListOfEnumsQuery(), schemaName)

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

	ret := []MetaData{}

	for enumName, enumValues := range enumsInfosMap {
		ret = append(ret, EnumMetaData{
			enumName,
			enumValues,
		})
	}

	return ret, nil
}
