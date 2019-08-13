package jet

// SelectTable is interface for SELECT sub-queries
type SelectTable interface {
	Alias() string
	AllColumns() ProjectionList
}

type SelectTableImpl struct {
	selectStmt StatementWithProjections
	alias      string

	projections []Projection
}

func NewSelectTable(selectStmt StatementWithProjections, alias string) SelectTableImpl {
	selectTable := SelectTableImpl{selectStmt: selectStmt, alias: alias}

	for _, projection := range selectStmt.projections() {
		newProjection := projection.fromImpl(&selectTable)

		selectTable.projections = append(selectTable.projections, newProjection)
	}

	return selectTable
}

func (s *SelectTableImpl) Alias() string {
	return s.alias
}

func (s *SelectTableImpl) AllColumns() ProjectionList {
	return s.projections
}

func (s *SelectTableImpl) serialize(statement StatementType, out *SqlBuilder, options ...SerializeOption) {
	if s == nil {
		panic("jet: expression table is nil. ")
	}

	s.selectStmt.serialize(statement, out)

	out.WriteString("AS")
	out.WriteIdentifier(s.alias)
}
