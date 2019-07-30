package execution

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"github.com/go-jet/jet/execution/internal"
	"github.com/go-jet/jet/internal/utils"
	"github.com/google/uuid"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// Query executes query with arguments over database connection with context and stores result into destination.
// Destination can be either pointer to struct or pointer to slice of structs.
func Query(context context.Context, db DB, query string, args []interface{}, destinationPtr interface{}) error {

	if utils.IsNil(destinationPtr) {
		return errors.New("jet: Destination is nil")
	}

	destinationPtrType := reflect.TypeOf(destinationPtr)
	if destinationPtrType.Kind() != reflect.Ptr {
		return errors.New("jet: Destination has to be a pointer to slice or pointer to struct")
	}

	if destinationPtrType.Elem().Kind() == reflect.Slice {
		return queryToSlice(context, db, query, args, destinationPtr)
	} else if destinationPtrType.Elem().Kind() == reflect.Struct {
		tempSlicePtrValue := reflect.New(reflect.SliceOf(destinationPtrType))
		tempSliceValue := tempSlicePtrValue.Elem()

		err := queryToSlice(context, db, query, args, tempSlicePtrValue.Interface())

		if err != nil {
			return err
		}

		if tempSliceValue.Len() == 0 {
			return nil
		}

		structValue := reflect.ValueOf(destinationPtr).Elem()
		firstTempStruct := tempSliceValue.Index(0).Elem()

		if structValue.Type().AssignableTo(firstTempStruct.Type()) {
			structValue.Set(tempSliceValue.Index(0).Elem())
		}
		return nil
	} else {
		return errors.New("jet: unsupported destination type")
	}
}

func queryToSlice(ctx context.Context, db DB, query string, args []interface{}, slicePtr interface{}) error {
	if db == nil {
		return errors.New("jet: db is nil")
	}

	if slicePtr == nil {
		return errors.New("jet: Destination is nil. ")
	}

	destinationType := reflect.TypeOf(slicePtr)
	if destinationType.Kind() != reflect.Ptr && destinationType.Elem().Kind() != reflect.Slice {
		return errors.New("jet: Destination has to be a pointer to slice. ")
	}

	if ctx == nil {
		ctx = context.Background()
	}

	rows, err := db.QueryContext(ctx, query, args...)

	if err != nil {
		return err
	}
	defer rows.Close()

	scanContext, err := newScanContext(rows)

	if err != nil {
		return err
	}

	if len(scanContext.row) == 0 {
		return nil
	}

	slicePtrValue := reflect.ValueOf(slicePtr)

	for rows.Next() {
		err := rows.Scan(scanContext.row...)

		if err != nil {
			return err
		}

		scanContext.rowNum++

		_, err = mapRowToSlice(scanContext, "", slicePtrValue, nil)

		if err != nil {
			return err
		}
	}

	err = rows.Close()
	if err != nil {
		return err
	}

	err = rows.Err()

	if err != nil {
		return err
	}

	return nil
}

func mapRowToSlice(scanContext *scanContext, groupKey string, slicePtrValue reflect.Value, field *reflect.StructField) (updated bool, err error) {

	sliceElemType := getSliceElemType(slicePtrValue)

	if isSimpleModelType(sliceElemType) {
		updated, err = mapRowToBaseTypeSlice(scanContext, slicePtrValue, field)
		return
	}

	if sliceElemType.Kind() != reflect.Struct {
		return false, errors.New("jet: Unsupported dest type: " + field.Name + " " + field.Type.String())
	}

	structGroupKey := scanContext.getGroupKey(sliceElemType, field)

	groupKey = groupKey + "," + structGroupKey

	index, ok := scanContext.uniqueDestObjectsMap[groupKey]

	if ok {
		structPtrValue := getSliceElemPtrAt(slicePtrValue, index)

		return mapRowToStruct(scanContext, groupKey, structPtrValue, field, true)
	}

	destinationStructPtr := newElemPtrValueForSlice(slicePtrValue)

	updated, err = mapRowToStruct(scanContext, groupKey, destinationStructPtr, field)

	if err != nil {
		return
	}

	if updated {
		scanContext.uniqueDestObjectsMap[groupKey] = slicePtrValue.Elem().Len()
		err = appendElemToSlice(slicePtrValue, destinationStructPtr)

		if err != nil {
			return
		}
	}

	return
}

func mapRowToBaseTypeSlice(scanContext *scanContext, slicePtrValue reflect.Value, field *reflect.StructField) (updated bool, err error) {
	index := 0
	if field != nil {
		typeName, columnName := getTypeAndFieldName("", *field)
		if index = scanContext.typeToColumnIndex(typeName, columnName); index < 0 {
			return
		}
	}
	rowElemPtr := scanContext.rowElemValuePtr(index)

	if !rowElemPtr.IsNil() {
		updated = true
		err = appendElemToSlice(slicePtrValue, rowElemPtr)
		if err != nil {
			return
		}
	}

	return
}

type typeInfo struct {
	fieldMappings []fieldMapping
}

type fieldMapping struct {
	complexType       bool
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

func mapRowToStruct(scanContext *scanContext, groupKey string, structPtrValue reflect.Value, parentField *reflect.StructField, onlySlices ...bool) (updated bool, err error) {
	structType := structPtrValue.Type().Elem()

	typeInf := scanContext.getTypeInfo(structType, parentField)

	structValue := structPtrValue.Elem()

	for i := 0; i < structValue.NumField(); i++ {
		field := structType.Field(i)
		fieldValue := structValue.Field(i)

		fieldMap := typeInf.fieldMappings[i]

		if fieldMap.complexType {
			var changed bool
			changed, err = mapRowToDestinationValue(scanContext, groupKey, fieldValue, &field)

			if err != nil {
				return
			}

			if changed {
				updated = true
			}

		} else if len(onlySlices) == 0 {

			if fieldMap.columnIndex == -1 {
				continue
			}

			if fieldMap.implementsScanner {

				cellValue := scanContext.rowElem(fieldMap.columnIndex)

				if cellValue == nil {
					continue
				}

				initializeValueIfNilPtr(fieldValue)

				scanner := getScanner(fieldValue)

				err = scanner.Scan(cellValue)

				if err != nil {
					err = fmt.Errorf("%s, at struct field: %s %s of type %s. ", err.Error(), field.Name, field.Type.String(), structType.String())
					return
				}
				updated = true
			} else {
				cellValue := scanContext.rowElem(fieldMap.columnIndex)

				if cellValue != nil {
					updated = true
					initializeValueIfNilPtr(fieldValue)
					err = setReflectValue(reflect.ValueOf(cellValue), fieldValue)

					if err != nil {
						err = fmt.Errorf("%s, at struct field: %s %s of type %s. ", err.Error(), field.Name, field.Type.String(), structType.String())
						return
					}
				}
			}
		}
	}

	return
}

func mapRowToDestinationPtr(scanContext *scanContext, groupKey string, destPtrValue reflect.Value, structField *reflect.StructField) (updated bool, err error) {

	if destPtrValue.Kind() != reflect.Ptr {
		return false, errors.New("jet: Internal error. ")
	}

	destValueKind := destPtrValue.Elem().Kind()

	if destValueKind == reflect.Struct {
		return mapRowToStruct(scanContext, groupKey, destPtrValue, structField)
	} else if destValueKind == reflect.Slice {
		return mapRowToSlice(scanContext, groupKey, destPtrValue, structField)
	} else {
		return false, errors.New("jet: Unsupported dest type: " + structField.Name + " " + structField.Type.String())
	}
}

func mapRowToDestinationValue(scanContext *scanContext, groupKey string, dest reflect.Value, structField *reflect.StructField) (updated bool, err error) {

	var destPtrValue reflect.Value

	if dest.Kind() != reflect.Ptr {
		destPtrValue = dest.Addr()
	} else if dest.Kind() == reflect.Ptr {
		if dest.IsNil() {
			destPtrValue = reflect.New(dest.Type().Elem())
		} else {
			destPtrValue = dest
		}
	} else {
		return false, errors.New("jet: Internal error. ")
	}

	updated, err = mapRowToDestinationPtr(scanContext, groupKey, destPtrValue, structField)

	if err != nil {
		return
	}

	if dest.Kind() == reflect.Ptr && dest.IsNil() && updated {
		dest.Set(destPtrValue)
	}

	return
}

var scannerInterfaceType = reflect.TypeOf((*sql.Scanner)(nil)).Elem()

func implementsScannerType(fieldType reflect.Type) bool {
	if fieldType.Implements(scannerInterfaceType) {
		return true
	}

	typePtr := reflect.New(fieldType).Type()

	return typePtr.Implements(scannerInterfaceType)
}

func getScanner(value reflect.Value) sql.Scanner {
	if scanner, ok := value.Interface().(sql.Scanner); ok {
		return scanner
	}

	return value.Addr().Interface().(sql.Scanner)
}

func getSliceElemType(slicePtrValue reflect.Value) reflect.Type {
	sliceTypePtr := slicePtrValue.Type()

	elemType := sliceTypePtr.Elem().Elem()

	if elemType.Kind() == reflect.Ptr {
		return elemType.Elem()
	}

	return elemType
}

func getSliceElemPtrAt(slicePtrValue reflect.Value, index int) reflect.Value {
	sliceValue := slicePtrValue.Elem()
	elem := sliceValue.Index(index)

	if elem.Kind() == reflect.Ptr {
		return elem
	}

	return elem.Addr()
}

func appendElemToSlice(slicePtrValue reflect.Value, objPtrValue reflect.Value) error {
	if slicePtrValue.IsNil() {
		panic("Slice is nil")
	}
	sliceValue := slicePtrValue.Elem()
	sliceElemType := sliceValue.Type().Elem()

	newElemValue := objPtrValue

	if sliceElemType.Kind() != reflect.Ptr {
		newElemValue = objPtrValue.Elem()
	}

	if !newElemValue.Type().AssignableTo(sliceElemType) {
		return fmt.Errorf("jet: can't append %s to %s slice ", newElemValue.Type().String(), sliceValue.Type().String())
	}

	sliceValue.Set(reflect.Append(sliceValue, newElemValue))

	return nil
}

func newElemPtrValueForSlice(slicePtrValue reflect.Value) reflect.Value {
	destinationSliceType := slicePtrValue.Type().Elem()
	elemType := indirectType(destinationSliceType.Elem())

	return reflect.New(elemType)
}

func getTypeName(structType reflect.Type, parentField *reflect.StructField) string {
	if parentField == nil {
		return structType.Name()
	}

	aliasTag := parentField.Tag.Get("alias")

	if aliasTag == "" {
		return structType.Name()
	}

	aliasParts := strings.Split(aliasTag, ".")

	return toCommonIdentifier(aliasParts[0])
}

func getTypeAndFieldName(structType string, field reflect.StructField) (string, string) {
	aliasTag := field.Tag.Get("alias")

	if aliasTag == "" {
		return structType, field.Name
	}

	aliasParts := strings.Split(aliasTag, ".")

	if len(aliasParts) == 1 {
		return structType, toCommonIdentifier(aliasParts[0])
	}

	return toCommonIdentifier(aliasParts[0]), toCommonIdentifier(aliasParts[1])
}

var replacer = strings.NewReplacer(" ", "", "-", "", "_", "")

func toCommonIdentifier(name string) string {
	return strings.ToLower(replacer.Replace(name))
}

func initializeValueIfNilPtr(value reflect.Value) {
	if !value.IsValid() || !value.CanSet() {
		return
	}

	if value.Kind() == reflect.Ptr && value.IsNil() {
		value.Set(reflect.New(value.Type().Elem()))
	}
}

func valueToString(value reflect.Value) string {

	if !value.IsValid() {
		return "nil"
	}

	var valueInterface interface{}
	if value.Kind() == reflect.Ptr {
		if value.IsNil() {
			return "nil"
		}
		valueInterface = value.Elem().Interface()
	} else {
		valueInterface = value.Interface()
	}

	if t, ok := valueInterface.(time.Time); ok {
		return t.String()
	}

	return fmt.Sprintf("%#v", valueInterface)
}

var timeType = reflect.TypeOf(time.Now())
var uuidType = reflect.TypeOf(uuid.New())

func isSimpleModelType(objType reflect.Type) bool {
	objType = indirectType(objType)

	switch objType.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64,
		reflect.String,
		reflect.Bool:
		return true
	case reflect.Slice:
		return objType.Elem().Kind() == reflect.Uint8 //[]byte
	case reflect.Struct:
		return objType == timeType || objType == uuidType // time.Time || uuid.UUID
	}

	return false
}

func isIntegerType(value reflect.Type) bool {
	switch value {
	case int8Type, unit8Type, int16Type, uint16Type,
		int32Type, uint32Type, int64Type, uint64Type:
		return true
	}

	return false
}

func tryAssign(source, destination reflect.Value) bool {
	if source.Type().ConvertibleTo(destination.Type()) {
		source = source.Convert(destination.Type())
	}

	if isIntegerType(source.Type()) && destination.Type() == boolType {
		intValue := source.Int()

		if intValue == 1 {
			source = reflect.ValueOf(true)
		} else if intValue == 0 {
			source = reflect.ValueOf(false)
		}
	}

	if source.Type().AssignableTo(destination.Type()) {
		destination.Set(source)
		return true
	}

	return false
}

func setReflectValue(source, destination reflect.Value) error {

	if tryAssign(source, destination) {
		return nil
	}

	if destination.Kind() == reflect.Ptr {
		if source.Kind() == reflect.Ptr {

			if !source.IsNil() {
				if destination.IsNil() {
					initializeValueIfNilPtr(destination)
				}

				tryAssign(source.Elem(), destination.Elem())
			}
		} else {
			if source.CanAddr() {
				source = source.Addr()
			} else {
				sourceCopy := reflect.New(source.Type())
				sourceCopy.Elem().Set(source)

				source = sourceCopy
			}

			if tryAssign(source, destination) {
				return nil
			}

			if tryAssign(source.Elem(), destination.Elem()) {
				return nil
			}
		}
	} else {
		if source.Kind() == reflect.Ptr {
			source = source.Elem()
		}

		if tryAssign(source, destination) {
			return nil
		}
	}

	return errors.New("jet: can't set " + source.Type().String() + " to " + destination.Type().String())
}

func createScanValue(columnTypes []*sql.ColumnType) []interface{} {
	values := make([]interface{}, len(columnTypes))

	for i, sqlColumnType := range columnTypes {
		columnType := newScanType(sqlColumnType)

		columnValue := reflect.New(columnType)

		values[i] = columnValue.Interface()
	}

	return values
}

var boolType = reflect.TypeOf(true)
var int8Type = reflect.TypeOf(int8(1))
var unit8Type = reflect.TypeOf(uint8(1))
var int16Type = reflect.TypeOf(int16(1))
var uint16Type = reflect.TypeOf(uint16(1))
var int32Type = reflect.TypeOf(int32(1))
var uint32Type = reflect.TypeOf(uint32(1))
var int64Type = reflect.TypeOf(int64(1))
var uint64Type = reflect.TypeOf(uint64(1))
var float32Type = reflect.TypeOf(float32(1.1))
var float64Type = reflect.TypeOf(float64(1.1))

var nullInt8Type = reflect.TypeOf(internal.NullInt8{})

//var nullUInt8Type = reflect.TypeOf(internal.NullUInt8{})
var nullInt16Type = reflect.TypeOf(internal.NullInt16{})

//var nullUInt16Type = reflect.TypeOf(internal.NullUInt16{})
var nullInt32Type = reflect.TypeOf(internal.NullInt32{})

//var nullUInt32Type = reflect.TypeOf(internal.NullUInt32{})
var nullInt64Type = reflect.TypeOf(sql.NullInt64{})

//var nullUInt64Type = reflect.TypeOf(internal.NullUInt64{})

var nullFloat32Type = reflect.TypeOf(internal.NullFloat32{})
var nullFloat64Type = reflect.TypeOf(sql.NullFloat64{})

var nullStringType = reflect.TypeOf(sql.NullString{})

var nullBoolType = reflect.TypeOf(sql.NullBool{})

var nullTimeType = reflect.TypeOf(internal.NullTime{})

var nullByteArrayType = reflect.TypeOf(internal.NullByteArray{})

func newScanType(columnType *sql.ColumnType) reflect.Type {

	switch columnType.DatabaseTypeName() {
	case "TINYINT":
		return nullInt8Type
	case "INT2", "SMALLINT", "YEAR":
		return nullInt16Type
	case "INT4", "MEDIUMINT", "INT":
		return nullInt32Type
	case "INT8", "BIGINT":
		return nullInt64Type
	case "CHAR", "VARCHAR", "TEXT", "", "_TEXT", "TSVECTOR", "BPCHAR", "UUID", "JSON", "JSONB", "INTERVAL", "POINT", "BIT", "VARBIT", "XML":
		return nullStringType
	case "FLOAT4":
		return nullFloat32Type
	case "FLOAT8", "NUMERIC", "DECIMAL", "FLOAT", "DOUBLE":
		return nullFloat64Type
	case "BOOL":
		return nullBoolType
	case "BYTEA", "BINARY", "VARBINARY", "BLOB":
		return nullByteArrayType
	case "DATE", "DATETIME", "TIMESTAMP", "TIMESTAMPTZ", "TIME", "TIMETZ":
		return nullTimeType
	default:
		return nullStringType
	}
}

type scanContext struct {
	rowNum int

	row                  []interface{}
	uniqueDestObjectsMap map[string]int

	typeToColumnIndexMap map[string]int

	groupKeyInfoCache map[string]groupKeyInfo

	typeInfoMap map[string]typeInfo
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

	typeToIndexMap := map[string]int{}

	for i, alias := range aliases {
		names := strings.SplitN(alias, ".", 2)

		goName := toCommonIdentifier(names[0])

		if len(names) > 1 {
			goName += "." + toCommonIdentifier(names[1])
		}

		typeToIndexMap[strings.ToLower(goName)] = i
	}

	return &scanContext{
		row:                  createScanValue(columnTypes),
		uniqueDestObjectsMap: make(map[string]int),

		groupKeyInfoCache:    make(map[string]groupKeyInfo),
		typeToColumnIndexMap: typeToIndexMap,

		typeInfoMap: make(map[string]typeInfo),
	}, nil
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
		return "|ROW: " + strconv.Itoa(s.rowNum) + "|"
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
	typeName := getTypeName(structType, parentField)

	ret := groupKeyInfo{typeName: structType.Name()}

	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)
		newTypeName, fieldName := getTypeAndFieldName(typeName, field)

		if !isSimpleModelType(field.Type) {
			var structType reflect.Type
			if field.Type.Kind() == reflect.Struct {
				structType = field.Type
			} else if field.Type.Kind() == reflect.Ptr && field.Type.Elem().Kind() == reflect.Struct {
				structType = field.Type.Elem()
			} else {
				continue
			}

			subType := s.getGroupKeyInfo(structType, &field)

			if len(subType.indexes) != 0 || len(subType.subTypes) != 0 {
				ret.subTypes = append(ret.subTypes, subType)
			}
		} else if isPrimaryKey(field) {
			index := s.typeToColumnIndex(newTypeName, fieldName)

			if index < 0 {
				continue
			}

			ret.indexes = append(ret.indexes, index)
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

	index, ok := s.typeToColumnIndexMap[key]

	if !ok {
		return -1
	}

	return index
}

func (s *scanContext) rowElem(index int) interface{} {

	valuer, ok := s.row[index].(driver.Valuer)

	if !ok {
		panic("Scan value doesn't implement driver.Valuer")
	}

	value, err := valuer.Value()

	if err != nil {
		panic(err)
	}

	return value
}

func (s *scanContext) rowElemValuePtr(index int) reflect.Value {
	rowElem := s.rowElem(index)
	rowElemValue := reflect.ValueOf(rowElem)

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

func isPrimaryKey(field reflect.StructField) bool {

	sqlTag := field.Tag.Get("sql")

	return sqlTag == "primary_key"
}

func indirectType(reflectType reflect.Type) reflect.Type {
	if reflectType.Kind() != reflect.Ptr {
		return reflectType
	}
	return reflectType.Elem()
}
