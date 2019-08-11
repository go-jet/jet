package jet

type Dialect interface {
	Name() string
	PackageName() string
	SerializeOverride(operator string) SerializeOverride
	AliasQuoteChar() byte
	IdentifierQuoteChar() byte
	ArgumentPlaceholder() QueryPlaceholderFunc
}

type SerializeFunc func(statement StatementType, out *SqlBuilder, options ...SerializeOption) error
type SerializeOverride func(expressions ...Expression) SerializeFunc

type QueryPlaceholderFunc func(ord int) string
type UpdateAssigmentFunc func(columns []Column, values []Serializer, out *SqlBuilder) (err error)

type DialectParams struct {
	Name                string
	PackageName         string
	SerializeOverrides  map[string]SerializeOverride
	AliasQuoteChar      byte
	IdentifierQuoteChar byte
	ArgumentPlaceholder QueryPlaceholderFunc
}

func NewDialect(params DialectParams) Dialect {
	return &dialectImpl{
		name:                params.Name,
		packageName:         params.PackageName,
		serializeOverrides:  params.SerializeOverrides,
		aliasQuoteChar:      params.AliasQuoteChar,
		identifierQuoteChar: params.IdentifierQuoteChar,
		argumentPlaceholder: params.ArgumentPlaceholder,
	}
}

type dialectImpl struct {
	name                string
	packageName         string
	serializeOverrides  map[string]SerializeOverride
	aliasQuoteChar      byte
	identifierQuoteChar byte
	argumentPlaceholder QueryPlaceholderFunc
	setClause           UpdateAssigmentFunc

	supportsReturning bool
}

func (d *dialectImpl) Name() string {
	return d.name
}

func (d *dialectImpl) PackageName() string {
	return d.packageName
}

func (d *dialectImpl) SerializeOverride(operator string) SerializeOverride {
	return d.serializeOverrides[operator]
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
