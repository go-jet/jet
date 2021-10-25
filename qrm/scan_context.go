package qrm

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
)

type scanContext struct {
	rowNum                   int64
	row                      []interface{}
	uniqueDestObjectsMap     map[string]int
	commonIdentToColumnIndex map[string]int
	groupKeyInfoCache        map[string]groupKeyInfo
	typeInfoMap              map[string]typeInfo
}

func newScanContext(rows *sql.Rows) (*scanContext, error) {
	aliases, err := rows.Columns()

	if err != nil {
		return nil, err
	}

	columnTypes, err := rows.ColumnTypes()

	if err != nil {
		return nil, err
	}

	commonIdentToColumnIndex := map[string]int{}

	for i, alias := range aliases {
		names := strings.SplitN(alias, ".", 2)
		commonIdentifier := toCommonIdentifier(names[0])

		if len(names) > 1 {
			commonIdentifier += "." + toCommonIdentifier(names[1])
		}

		commonIdentToColumnIndex[commonIdentifier] = i
	}

	return &scanContext{
		row:                  createScanSlice(len(columnTypes)),
		uniqueDestObjectsMap: make(map[string]int),

		groupKeyInfoCache:        make(map[string]groupKeyInfo),
		commonIdentToColumnIndex: commonIdentToColumnIndex,

		typeInfoMap: make(map[string]typeInfo),
	}, nil
}

func createScanSlice(columnCount int) []interface{} {
	scanSlice := make([]interface{}, columnCount)
	scanPtrSlice := make([]interface{}, columnCount)

	for i := range scanPtrSlice {
		scanPtrSlice[i] = &scanSlice[i] // if destination is pointer to interface sql.Scan will just forward driver value
	}

	return scanPtrSlice
}

type typeInfo struct {
	fieldMappings []fieldMapping
}

type fieldMapping struct {
	complexType       bool // slice or struct
	columnIndex       int
	implementsScanner bool
}

func (s *scanContext) getTypeInfo(structType reflect.Type, parentField *reflect.StructField) typeInfo {

	typeMapKey := structType.String()

	if parentField != nil {
		typeMapKey += string(parentField.Tag)
	}

	if typeInfo, ok := s.typeInfoMap[typeMapKey]; ok {
		return typeInfo
	}

	typeName := getTypeName(structType, parentField)

	newTypeInfo := typeInfo{}

	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)

		newTypeName, fieldName := getTypeAndFieldName(typeName, field)
		columnIndex := s.typeToColumnIndex(newTypeName, fieldName)

		fieldMap := fieldMapping{
			columnIndex: columnIndex,
		}

		if implementsScannerType(field.Type) {
			fieldMap.implementsScanner = true
		} else if !isSimpleModelType(field.Type) {
			fieldMap.complexType = true
		}

		newTypeInfo.fieldMappings = append(newTypeInfo.fieldMappings, fieldMap)
	}

	s.typeInfoMap[typeMapKey] = newTypeInfo

	return newTypeInfo
}

type groupKeyInfo struct {
	typeName string
	indexes  []int
	subTypes []groupKeyInfo
}

func (s *scanContext) getGroupKey(structType reflect.Type, structField *reflect.StructField) string {

	mapKey := structType.Name()

	if structField != nil {
		mapKey += structField.Type.String()
	}

	if groupKeyInfo, ok := s.groupKeyInfoCache[mapKey]; ok {
		return s.constructGroupKey(groupKeyInfo)
	}

	groupKeyInfo := s.getGroupKeyInfo(structType, structField)

	s.groupKeyInfoCache[mapKey] = groupKeyInfo

	return s.constructGroupKey(groupKeyInfo)
}

func (s *scanContext) constructGroupKey(groupKeyInfo groupKeyInfo) string {
	if len(groupKeyInfo.indexes) == 0 && len(groupKeyInfo.subTypes) == 0 {
		return fmt.Sprintf("|ROW:%d|", s.rowNum)
	}

	groupKeys := []string{}

	for _, index := range groupKeyInfo.indexes {
		cellValue := s.rowElem(index)
		subKey := valueToString(reflect.ValueOf(cellValue))

		groupKeys = append(groupKeys, subKey)
	}

	subTypesGroupKeys := []string{}
	for _, subType := range groupKeyInfo.subTypes {
		subTypesGroupKeys = append(subTypesGroupKeys, s.constructGroupKey(subType))
	}

	return groupKeyInfo.typeName + "(" + strings.Join(groupKeys, ",") + strings.Join(subTypesGroupKeys, ",") + ")"
}

func (s *scanContext) getGroupKeyInfo(structType reflect.Type, parentField *reflect.StructField) groupKeyInfo {
	ret := groupKeyInfo{typeName: structType.Name()}

	typeName := getTypeName(structType, parentField)
	primaryKeyOverwrites := parentFieldPrimaryKeyOverwrite(parentField)

	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)
		fieldType := indirectType(field.Type)

		if !isSimpleModelType(fieldType) {
			if fieldType.Kind() != reflect.Struct {
				continue
			}

			subType := s.getGroupKeyInfo(fieldType, &field)

			if len(subType.indexes) != 0 || len(subType.subTypes) != 0 {
				ret.subTypes = append(ret.subTypes, subType)
			}
		} else {
			if isPrimaryKey(field, primaryKeyOverwrites) {
				newTypeName, fieldName := getTypeAndFieldName(typeName, field)

				index := s.typeToColumnIndex(newTypeName, fieldName)

				if index < 0 {
					continue
				}

				ret.indexes = append(ret.indexes, index)
			}
		}
	}

	return ret
}

func (s *scanContext) typeToColumnIndex(typeName, fieldName string) int {
	var key string

	if typeName != "" {
		key = strings.ToLower(typeName + "." + fieldName)
	} else {
		key = strings.ToLower(fieldName)
	}

	index, ok := s.commonIdentToColumnIndex[key]

	if !ok {
		return -1
	}

	return index
}

func (s *scanContext) rowElem(index int) interface{} {
	cellValue := reflect.ValueOf(s.row[index])

	if cellValue.IsValid() && !cellValue.IsNil() {
		return cellValue.Elem().Interface()
	}

	return nil
}

func (s *scanContext) rowElemValuePtr(index int) reflect.Value {
	rowElem := s.rowElem(index)
	rowElemValue := reflect.ValueOf(rowElem)

	if !rowElemValue.IsValid() {
		return reflect.Value{}
	}

	if rowElemValue.Kind() == reflect.Ptr {
		return rowElemValue
	}

	if rowElemValue.CanAddr() {
		return rowElemValue.Addr()
	}

	newElem := reflect.New(rowElemValue.Type())
	newElem.Elem().Set(rowElemValue)
	return newElem
}
