package jet

import "strings"

// Dialect interface
type Dialect interface {
	Name() string
	PackageName() string
	OperatorSerializeOverride(operator string) SerializeOverride
	FunctionSerializeOverride(function string) SerializeOverride
	AliasQuoteChar() byte
	IdentifierQuoteChar() byte
	ArgumentPlaceholder() QueryPlaceholderFunc
	IsReservedWord(name string) bool
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
	ReservedWords              []string
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
		reservedWords:              arrayOfStringsToMapOfStrings(params.ReservedWords),
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
	reservedWords              map[string]bool

	supportsReturning bool
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

func (d *dialectImpl) IsReservedWord(name string) bool {
	_, isReservedWord := d.reservedWords[strings.ToLower(name)]
	return isReservedWord
}

func arrayOfStringsToMapOfStrings(arr []string) map[string]bool {
	ret := map[string]bool{}
	for _, elem := range arr {
		ret[strings.ToLower(elem)] = true
	}

	return ret
}
