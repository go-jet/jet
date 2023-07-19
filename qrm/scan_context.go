package qrm

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
)

// ScanContext  contains information about current row processed, mapping from the row to the
// destination types and type grouping information.
type ScanContext struct {
	rowNum                   int64
	row                      []interface{}
	uniqueDestObjectsMap     map[string]int
	commonIdentToColumnIndex map[string]int
	groupKeyInfoCache        map[string]groupKeyInfo
	typeInfoMap              map[string]typeInfo

	typesVisited typeStack // to prevent circular dependency scan
}

// NewScanContext creates new ScanContext from rows
func NewScanContext(rows *sql.Rows) (*ScanContext, error) {
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
			commonIdentifier = concat(commonIdentifier, ".", toCommonIdentifier(names[1]))
		}

		commonIdentToColumnIndex[commonIdentifier] = i
	}

	return &ScanContext{
		row:                  createScanSlice(len(columnTypes)),
		uniqueDestObjectsMap: make(map[string]int),

		groupKeyInfoCache:        make(map[string]groupKeyInfo),
		commonIdentToColumnIndex: commonIdentToColumnIndex,

		typeInfoMap: make(map[string]typeInfo),

		typesVisited: newTypeStack(),
	}, nil
}

func createScanSlice(columnCount int) []interface{} {
	scanPtrSlice := make([]interface{}, columnCount)

	for i := range scanPtrSlice {
		var a interface{}
		scanPtrSlice[i] = &a // if destination is pointer to interface sql.Scan will just forward driver value
	}

	return scanPtrSlice
}

type typeInfo struct {
	fieldMappings []fieldMapping
}

type fieldMapping struct {
	complexType       bool // slice and struct are complex types
	rowIndex          int  // index in ScanContext.row
	implementsScanner bool
}

func (s *ScanContext) getTypeInfo(structType reflect.Type, parentField *reflect.StructField) typeInfo {

	typeMapKey := structType.String()

	if parentField != nil {
		typeMapKey = concat(typeMapKey, string(parentField.Tag))
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
			rowIndex: columnIndex,
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
	typeName  string
	pkIndexes []int
	subTypes  []groupKeyInfo
}

func (s *ScanContext) getGroupKey(structType reflect.Type, structField *reflect.StructField) string {

	mapKey := structType.Name()

	if structField != nil {
		mapKey = concat(mapKey, structField.Type.String(), string(structField.Tag))
	}

	if groupKeyInfo, ok := s.groupKeyInfoCache[mapKey]; ok {
		return s.constructGroupKey(groupKeyInfo)
	}

	tempTypeStack := newTypeStack()
	groupKeyInfo := s.getGroupKeyInfo(structType, structField, &tempTypeStack)

	s.groupKeyInfoCache[mapKey] = groupKeyInfo

	return s.constructGroupKey(groupKeyInfo)
}

func (s *ScanContext) constructGroupKey(groupKeyInfo groupKeyInfo) string {
	if len(groupKeyInfo.pkIndexes) == 0 && len(groupKeyInfo.subTypes) == 0 {
		return fmt.Sprintf("|ROW:%d|", s.rowNum)
	}

	var groupKeys []string

	for _, index := range groupKeyInfo.pkIndexes {
		groupKeys = append(groupKeys, s.rowElemToString(index))
	}

	var subTypesGroupKeys []string
	for _, subType := range groupKeyInfo.subTypes {
		subTypesGroupKeys = append(subTypesGroupKeys, s.constructGroupKey(subType))
	}

	return concat(groupKeyInfo.typeName, "(", strings.Join(groupKeys, ","), strings.Join(subTypesGroupKeys, ","), ")")
}

func (s *ScanContext) getGroupKeyInfo(
	structType reflect.Type,
	parentField *reflect.StructField,
	typeVisited *typeStack) groupKeyInfo {

	ret := groupKeyInfo{typeName: structType.Name()}

	if typeVisited.contains(&structType) {
		return ret
	}

	typeVisited.push(&structType)
	defer typeVisited.pop()

	typeName := getTypeName(structType, parentField)
	primaryKeyOverwrites := parentFieldPrimaryKeyOverwrite(parentField)

	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)
		fieldType := indirectType(field.Type)

		if isPrimaryKey(field, primaryKeyOverwrites) {
			newTypeName, fieldName := getTypeAndFieldName(typeName, field)

			pkIndex := s.typeToColumnIndex(newTypeName, fieldName)

			if pkIndex < 0 {
				continue
			}

			ret.pkIndexes = append(ret.pkIndexes, pkIndex)

		} else if fieldType.Kind() == reflect.Struct && fieldType != timeType {

			subType := s.getGroupKeyInfo(fieldType, &field, typeVisited)

			if len(subType.pkIndexes) != 0 || len(subType.subTypes) != 0 {
				ret.subTypes = append(ret.subTypes, subType)
			}
		}
	}

	return ret
}

func (s *ScanContext) typeToColumnIndex(typeName, fieldName string) int {
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

// rowElemValue always returns non-ptr value,
// invalid value is nil
func (s *ScanContext) rowElemValue(index int) reflect.Value {
	scannedValue := reflect.ValueOf(s.row[index])
	return scannedValue.Elem().Elem() // no need to check validity of Elem, because s.row[index] always contains interface in interface
}

func (s *ScanContext) rowElemToString(index int) string {
	value := s.rowElemValue(index)

	if !value.IsValid() {
		return "nil"
	}

	valueInterface := value.Interface()

	if t, ok := valueInterface.(fmt.Stringer); ok {
		return t.String()
	}

	return fmt.Sprintf("%#v", valueInterface)
}

func (s *ScanContext) rowElemValueClonePtr(index int) reflect.Value {
	rowElemValue := s.rowElemValue(index)

	if !rowElemValue.IsValid() {
		return reflect.Value{}
	}

	newElem := reflect.New(rowElemValue.Type())
	newElem.Elem().Set(rowElemValue)
	return newElem
}
