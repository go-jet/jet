package jet

import (
	"strings"
)

// Dialect interface
type Dialect interface {
	Name() string
	PackageName() string
	OperatorSerializeOverride(operator string) SerializeOverride
	FunctionSerializeOverride(function string) SerializeOverride
	AliasQuoteChar() byte
	IdentifierQuoteChar() byte
	ArgumentPlaceholder() QueryPlaceholderFunc
	ArgumentToString(value any) (string, bool)
	IsReservedWord(name string) bool
	SerializeOrderBy() func(expression Expression, ascending, nullsFirst *bool) SerializerFunc
	ValuesDefaultColumnName(index int) string
	JsonValueEncode(expr Expression) Expression
}

// SerializerFunc func
type SerializerFunc func(statement StatementType, out *SQLBuilder, options ...SerializeOption)

// SerializeOverride func
type SerializeOverride func(expressions ...Serializer) SerializerFunc

// QueryPlaceholderFunc func
type QueryPlaceholderFunc func(ord int) string

// DialectParams struct
type DialectParams struct {
	Name                       string
	PackageName                string
	OperatorSerializeOverrides map[string]SerializeOverride
	FunctionSerializeOverrides map[string]SerializeOverride
	AliasQuoteChar             byte
	IdentifierQuoteChar        byte
	ArgumentPlaceholder        QueryPlaceholderFunc
	ArgumentToString           func(value any) (string, bool)
	ReservedWords              []string
	SerializeOrderBy           func(expression Expression, ascending, nullsFirst *bool) SerializerFunc
	ValuesDefaultColumnName    func(index int) string
	JsonValueEncode            func(expr Expression) Expression
}

// NewDialect creates new dialect with params
func NewDialect(params DialectParams) Dialect {
	return &dialectImpl{
		name:                       params.Name,
		packageName:                params.PackageName,
		operatorSerializeOverrides: params.OperatorSerializeOverrides,
		functionSerializeOverrides: params.FunctionSerializeOverrides,
		aliasQuoteChar:             params.AliasQuoteChar,
		identifierQuoteChar:        params.IdentifierQuoteChar,
		argumentPlaceholder:        params.ArgumentPlaceholder,
		argumentToString:           params.ArgumentToString,
		reservedWords:              arrayOfStringsToMapOfStrings(params.ReservedWords),
		serializeOrderBy:           params.SerializeOrderBy,
		valuesDefaultColumnName:    params.ValuesDefaultColumnName,
		jsonValueEncode:            params.JsonValueEncode,
	}
}

type dialectImpl struct {
	name                       string
	packageName                string
	operatorSerializeOverrides map[string]SerializeOverride
	functionSerializeOverrides map[string]SerializeOverride
	aliasQuoteChar             byte
	identifierQuoteChar        byte
	argumentPlaceholder        QueryPlaceholderFunc
	argumentToString           func(value any) (string, bool)
	reservedWords              map[string]bool
	serializeOrderBy           func(expression Expression, ascending, nullsFirst *bool) SerializerFunc
	valuesDefaultColumnName    func(index int) string
	jsonValueEncode            func(expr Expression) Expression
}

func (d *dialectImpl) Name() string {
	return d.name
}

func (d *dialectImpl) PackageName() string {
	return d.packageName
}

func (d *dialectImpl) OperatorSerializeOverride(operator string) SerializeOverride {
	if d.operatorSerializeOverrides == nil {
		return nil
	}
	return d.operatorSerializeOverrides[operator]
}

func (d *dialectImpl) FunctionSerializeOverride(function string) SerializeOverride {
	if d.functionSerializeOverrides == nil {
		return nil
	}
	return d.functionSerializeOverrides[function]
}

func (d *dialectImpl) AliasQuoteChar() byte {
	return d.aliasQuoteChar
}

func (d *dialectImpl) IdentifierQuoteChar() byte {
	return d.identifierQuoteChar
}

func (d *dialectImpl) ArgumentPlaceholder() QueryPlaceholderFunc {
	return d.argumentPlaceholder
}

func (d *dialectImpl) ArgumentToString(value any) (string, bool) {
	return d.argumentToString(value)
}

func (d *dialectImpl) IsReservedWord(name string) bool {
	_, isReservedWord := d.reservedWords[strings.ToLower(name)]
	return isReservedWord
}

func (d *dialectImpl) SerializeOrderBy() func(expression Expression, ascending, nullsFirst *bool) SerializerFunc {
	return d.serializeOrderBy
}

func (d *dialectImpl) ValuesDefaultColumnName(index int) string {
	return d.valuesDefaultColumnName(index)
}

func (d *dialectImpl) JsonValueEncode(expr Expression) Expression {
	return d.jsonValueEncode(expr)
}

func arrayOfStringsToMapOfStrings(arr []string) map[string]bool {
	ret := map[string]bool{}
	for _, elem := range arr {
		ret[strings.ToLower(elem)] = true
	}

	return ret
}
