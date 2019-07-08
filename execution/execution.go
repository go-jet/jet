package execution

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"github.com/go-jet/jet/execution/internal"
	"github.com/go-jet/jet/internal/utils"
	"reflect"
	"strconv"
	"strings"
	"time"
)

func Query(db DB, context context.Context, query string, args []interface{}, destinationPtr interface{}) error {

	if destinationPtr == nil {
		return errors.New("jet: Destination is nil.")
	}

	destinationPtrType := reflect.TypeOf(destinationPtr)
	if destinationPtrType.Kind() != reflect.Ptr {
		return errors.New("jet: Destination has to be a pointer to slice or pointer to struct")
	}

	if destinationPtrType.Elem().Kind() == reflect.Slice {
		return queryToSlice(db, context, query, args, destinationPtr)
	} else if destinationPtrType.Elem().Kind() == reflect.Struct {
		tempSlicePtrValue := reflect.New(reflect.SliceOf(destinationPtrType))
		tempSliceValue := tempSlicePtrValue.Elem()

		err := queryToSlice(db, context, query, args, tempSlicePtrValue.Interface())

		if err != nil {
			return err
		}

		fmt.Println("TEMP SLICE SIZE: ", tempSliceValue.Len())

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

func queryToSlice(db DB, ctx context.Context, query string, args []interface{}, slicePtr interface{}) error {
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

	groupTime := time.Duration(0)

	slicePtrValue := reflect.ValueOf(slicePtr)

	for rows.Next() {
		err := rows.Scan(scanContext.row...)

		if err != nil {
			return err
		}

		scanContext.rowNum++

		begin := time.Now()

		_, err = mapRowToSlice(scanContext, "", slicePtrValue, nil)

		if err != nil {
			return err
		}

		groupTime += time.Now().Sub(begin)
	}

	fmt.Println(groupTime.String())

	err = rows.Close()
	if err != nil {
		return err
	}

	err = rows.Err()

	if err != nil {
		return err
	}

	fmt.Println(strconv.Itoa(scanContext.rowNum) + " ROW(S) PROCESSED")

	return nil
}

func mapRowToSlice(scanContext *scanContext, groupKey string, slicePtrValue reflect.Value, structField *reflect.StructField) (updated bool, err error) {

	sliceElemType := getSliceElemType(slicePtrValue)

	if isGoBaseType(sliceElemType) {
		index := 0
		if structField != nil {
			if index = scanContext.aliasColumnIndex(structField.Tag.Get("alias")); index < 0 {
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

	if sliceElemType.Kind() != reflect.Struct {
		return false, errors.New("jet: Unsupported dest type: " + structField.Name + " " + structField.Type.String())
	}

	structGroupKey := scanContext.getGroupKey(sliceElemType, structField)

	groupKey = groupKey + ":" + structGroupKey

	index, ok := scanContext.uniqueDestObjectsMap[groupKey]

	if ok {
		structPtrValue := getSliceElemPtrAt(slicePtrValue, index)

		return mapRowToStruct(scanContext, groupKey, structPtrValue, structField, true)
	} else {
		destinationStructPtr := newElemPtrValueForSlice(slicePtrValue)

		updated, err = mapRowToStruct(scanContext, groupKey, destinationStructPtr, structField)

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
	}

	return
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

func mapRowToStruct(scanContext *scanContext, groupKey string, structPtrValue reflect.Value, parentField *reflect.StructField, onlySlices ...bool) (updated bool, err error) {
	structType := structPtrValue.Type().Elem()
	structValue := structPtrValue.Elem()

	typeName := getTypeName(structType, parentField)

	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)

		fieldValue := structValue.Field(i)
		fieldName := field.Name

		if scannerValue, ok := implementsScanner(fieldValue); ok {
			if len(onlySlices) > 0 {
				continue
			}

			cellValue := scanContext.getCellValue(typeName, fieldName)

			if cellValue == nil {
				continue
			}

			initializeValueIfNilPtr(fieldValue)

			scanner := scannerValue.Interface().(sql.Scanner)

			err = scanner.Scan(cellValue)

			if err != nil {
				err = fmt.Errorf("%s, at struct field: %s %s of type %s. ", err.Error(), field.Name, field.Type.String(), structType.String())
				return
			}
			updated = true
		} else if isGoBaseType(field.Type) {
			if len(onlySlices) > 0 {
				continue
			}

			cellValue := scanContext.getCellValue(typeName, fieldName)

			if cellValue != nil {
				updated = true
				initializeValueIfNilPtr(fieldValue)
				err = setReflectValue(reflect.ValueOf(cellValue), fieldValue)

				if err != nil {
					err = fmt.Errorf("%s, at struct field: %s %s of type %s. ", err.Error(), field.Name, field.Type.String(), structType.String())
					return
				}
			}
		} else {
			var changed bool
			changed, err = mapRowToDestinationValue(scanContext, groupKey, fieldValue, &field)

			if err != nil {
				return
			}

			if changed {
				updated = true
			}
		}
	}

	return
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

	return aliasParts[0]
}

func initializeValueIfNilPtr(value reflect.Value) {
	if !value.IsValid() || !value.CanSet() {
		return
	}

	if value.Kind() == reflect.Ptr && value.IsNil() {
		value.Set(reflect.New(value.Type().Elem()))
	}
}

func implementsScanner(value reflect.Value) (reflect.Value, bool) {
	if _, ok := value.Interface().(sql.Scanner); ok {
		return value, true
	} else if value.CanAddr() {
		if _, ok := value.Addr().Interface().(sql.Scanner); ok {
			return value.Addr(), true
		}
	}

	return value, false
}

func valueToString(value reflect.Value) string {

	if !value.IsValid() {
		return "nil"
	}

	var valueInterface interface{}
	if value.Kind() == reflect.Ptr {
		if value.IsNil() {
			return "nil"
		} else {
			valueInterface = value.Elem().Interface()
		}
	} else {
		valueInterface = value.Interface()
	}

	if t, ok := valueInterface.(time.Time); ok {
		return t.String()
	}

	return fmt.Sprintf("%#v", valueInterface)
}

func isGoBaseType(objType reflect.Type) bool {
	typeStr := objType.String()

	switch typeStr {
	case "string", "int", "int16", "int32", "int64", "float32", "float64", "time.Time", "bool", "[]byte", "[]uint8",
		"*string", "*int", "*int16", "*int32", "*int64", "*float32", "*float64", "*time.Time", "*bool", "*[]byte", "*[]uint8":
		return true
	}

	return false
}

func setReflectValue(source, destination reflect.Value) error {
	var sourceElem reflect.Value
	if destination.Kind() == reflect.Ptr {
		if source.Kind() == reflect.Ptr {
			sourceElem = source
		} else {
			if source.CanAddr() {
				sourceElem = source.Addr()
			} else {
				sourceCopy := reflect.New(source.Type())
				sourceCopy.Elem().Set(source)

				sourceElem = sourceCopy
			}
		}
	} else {
		if source.Kind() == reflect.Ptr {
			sourceElem = source.Elem()
		} else {
			sourceElem = source
		}
	}

	if !sourceElem.Type().AssignableTo(destination.Type()) {
		return errors.New("jet: can't set " + sourceElem.Type().String() + " to " + destination.Type().String())
	}

	destination.Set(sourceElem)

	return nil
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

var nullFloatType = reflect.TypeOf(internal.NullFloat32{})
var nullFloat64Type = reflect.TypeOf(sql.NullFloat64{})
var nullInt16Type = reflect.TypeOf(internal.NullInt16{})
var nullInt32Type = reflect.TypeOf(internal.NullInt32{})
var nullInt64Type = reflect.TypeOf(sql.NullInt64{})
var nullStringType = reflect.TypeOf(sql.NullString{})
var nullBoolType = reflect.TypeOf(sql.NullBool{})
var nullTimeType = reflect.TypeOf(internal.NullTime{})
var nullByteArrayType = reflect.TypeOf(internal.NullByteArray{})

func newScanType(columnType *sql.ColumnType) reflect.Type {
	switch columnType.DatabaseTypeName() {
	case "INT2":
		return nullInt16Type
	case "INT4":
		return nullInt32Type
	case "INT8":
		return nullInt64Type
	case "VARCHAR", "TEXT", "", "_TEXT", "TSVECTOR", "BPCHAR", "UUID", "JSON", "JSONB", "INTERVAL", "POINT", "BIT", "VARBIT", "XML":
		return nullStringType
	case "FLOAT4":
		return nullFloatType
	case "FLOAT8", "NUMERIC", "DECIMAL":
		return nullFloat64Type
	case "BOOL":
		return nullBoolType
	case "BYTEA":
		return nullByteArrayType
	case "DATE", "TIMESTAMP", "TIMESTAMPTZ", "TIME", "TIMETZ":
		return nullTimeType
	default:
		fmt.Println("Unknown column database type " + columnType.DatabaseTypeName() + " using string as default.")
		return nullStringType
	}
}

type scanContext struct {
	rowNum int

	row                  []interface{}
	uniqueDestObjectsMap map[string]int

	aliasIndexMap map[string]int
	goNameMap     map[string]int

	groupKeyInfoCache map[string]groupKeyInfo
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

	aliasIndexMap := map[string]int{}

	for i, columnName := range aliases {
		aliasIndexMap[strings.ToLower(columnName)] = i
	}

	goNamesMap := map[string]int{}

	for i, alias := range aliases {
		names := strings.SplitN(alias, ".", 2)

		goName := utils.ToGoIdentifier(names[0])

		if len(names) > 1 {
			goName += "." + utils.ToGoIdentifier(names[1])
		}

		goNamesMap[strings.ToLower(goName)] = i
	}

	return &scanContext{
		row:                  createScanValue(columnTypes),
		uniqueDestObjectsMap: make(map[string]int),

		groupKeyInfoCache: make(map[string]groupKeyInfo),
		aliasIndexMap:     aliasIndexMap,
		goNameMap:         goNamesMap,
	}, nil
}

func (s *scanContext) getGroupKey(structType reflect.Type, structField *reflect.StructField) string {

	mapKey := structType.Name()

	if structField != nil {
		mapKey += structField.Type.String()
	}

	if groupKeyInfo, ok := s.groupKeyInfoCache[mapKey]; ok {
		return s.constructGroupKey(groupKeyInfo)
	} else {
		groupKeyInfo := s.getGroupKeyInfo(structType, structField)

		s.groupKeyInfoCache[mapKey] = groupKeyInfo

		return s.constructGroupKey(groupKeyInfo)
	}
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

	return "{" + groupKeyInfo.typeName + "(" + strings.Join(groupKeys, ",") + strings.Join(subTypesGroupKeys, ",") + ")}"
}

func (s *scanContext) getGroupKeyInfo(structType reflect.Type, parentField *reflect.StructField) groupKeyInfo {
	typeName := getTypeName(structType, parentField)

	ret := groupKeyInfo{typeName: structType.Name()}

	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)

		if !isGoBaseType(field.Type) {
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
			index := s.typeColumnIndex(typeName, field.Name)

			if index < 0 {
				continue
			}

			ret.indexes = append(ret.indexes, index)
		}
	}

	return ret
}

type groupKeyInfo struct {
	typeName string
	indexes  []int
	subTypes []groupKeyInfo
}

func (s *scanContext) aliasColumnIndex(alias string) int {
	index, ok := s.aliasIndexMap[alias]

	if !ok {
		return -1
	}

	return index
}

func (s *scanContext) typeColumnIndex(typeName, fieldName string) int {
	var key string

	if typeName != "" {
		key = strings.ToLower(typeName + "." + fieldName)
	} else {
		key = strings.ToLower(fieldName)
	}

	index, ok := s.goNameMap[key]

	if !ok {
		return -1
	}

	return index
}

func (s *scanContext) getCellValue(typeName, fieldName string) interface{} {
	index := s.typeColumnIndex(typeName, fieldName)

	if index < 0 {
		return nil
	}

	return s.rowElem(index)
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
