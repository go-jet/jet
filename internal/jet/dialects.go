package jet

var ANSII = NewDialect(DialectParams{ // just for tests
	AliasQuoteChar: '"',
	ArgumentPlaceholder: func(ord int) string {
		return "#"
	},
})

type Dialect interface {
	Name() string
	PackageName() string
	SerializeOverride(operator string) SerializeOverride
	CastOverride() CastOverride
	AliasQuoteChar() byte
	IdentifierQuoteChar() byte
	ArgumentPlaceholder() QueryPlaceholderFunc
	UpdateAssigment() func(columns []IColumn, values []Clause, out *SqlBuilder) (err error)
	SupportsReturning() bool
}

type SerializeFunc func(statement StatementType, out *SqlBuilder, options ...SerializeOption) error
type SerializeOverride func(expressions ...Expression) SerializeFunc

type QueryPlaceholderFunc func(ord int) string

type CastOverride func(expression Expression, castType string) SerializeFunc
type UpdateAssigmentFunc func(columns []IColumn, values []Clause, out *SqlBuilder) (err error)

type DialectParams struct {
	Name                string
	PackageName         string
	SerializeOverrides  map[string]SerializeOverride
	CastOverride        CastOverride
	AliasQuoteChar      byte
	IdentifierQuoteChar byte
	ArgumentPlaceholder QueryPlaceholderFunc
	UpdateAssigment     func(columns []IColumn, values []Clause, out *SqlBuilder) (err error)

	SupportsReturning bool
}

func NewDialect(params DialectParams) Dialect {
	return &dialectImpl{
		name:                params.Name,
		packageName:         params.PackageName,
		serializeOverrides:  params.SerializeOverrides,
		castOverride:        params.CastOverride,
		aliasQuoteChar:      params.AliasQuoteChar,
		identifierQuoteChar: params.IdentifierQuoteChar,
		argumentPlaceholder: params.ArgumentPlaceholder,
		updateAssigment:     params.UpdateAssigment,
		supportsReturning:   params.SupportsReturning,
	}
}

type dialectImpl struct {
	name                string
	packageName         string
	serializeOverrides  map[string]SerializeOverride
	castOverride        CastOverride
	aliasQuoteChar      byte
	identifierQuoteChar byte
	argumentPlaceholder QueryPlaceholderFunc
	updateAssigment     UpdateAssigmentFunc

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

func (d *dialectImpl) CastOverride() CastOverride {
	return d.castOverride
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

func (d *dialectImpl) UpdateAssigment() func(columns []IColumn, values []Clause, out *SqlBuilder) (err error) {
	return d.updateAssigment
}

func (d *dialectImpl) SupportsReturning() bool {
	return d.supportsReturning
}
