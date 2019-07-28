package jet

type visitor interface {
	visit(element acceptsVisitor)
}

type acceptsVisitor interface {
	accept(visitor visitor)
}

type noOpVisitorImpl struct {
}

func (n *noOpVisitorImpl) accept(visitor visitor) {
	// NO OP
}

// --------------- dialect finder -----------------//

type DialectFinder struct {
	dialects map[string]Dialect
}

func newDialectFinder() *DialectFinder {
	return &DialectFinder{
		dialects: make(map[string]Dialect),
	}
}

func (f *DialectFinder) dialect() Dialect {
	if len(f.dialects) == 0 {
		panic("jet: can't detect dialect")
	}

	if len(f.dialects) > 1 {
		panic("jet: more than one dialect detected")
	}

	for _, dialect := range f.dialects {
		return dialect
	}

	panic("jet: internal error")
}

func (f *DialectFinder) visit(element acceptsVisitor) {

	if table, ok := element.(table); ok {
		dialect := table.dialect()
		f.dialects[dialect.Name] = dialect
	}
}

func detectDialect(element acceptsVisitor, dialectOverride ...Dialect) Dialect {
	if len(dialectOverride) > 0 {
		return dialectOverride[0]
	}

	dialectFinder := newDialectFinder()
	element.accept(dialectFinder)

	return dialectFinder.dialect()
}
