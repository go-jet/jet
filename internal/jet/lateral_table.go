package jet

// LateralTable is interface for Lateral sub-queries
type LateralTable interface {
	Serializer
	Alias() string
	AllColumns() ProjectionList
}

type lateralTableImpl struct {
	selectStmt SerializerStatement
	alias      string
}

// NewLateralTable func
func NewLateralTable(selectStmt SerializerStatement, alias string) LateralTable {
	lateralTable := &lateralTableImpl{selectStmt: selectStmt, alias: alias}
	return lateralTable
}

func (s lateralTableImpl) Alias() string {
	return s.alias
}

func (s lateralTableImpl) AllColumns() ProjectionList {
	statementWithProjections, ok := s.selectStmt.(HasProjections)
	if !ok {
		return ProjectionList{}
	}

	projectionList := statementWithProjections.projections().fromImpl(s)
	return projectionList.(ProjectionList)
}

func (s lateralTableImpl) serialize(statement StatementType, out *SQLBuilder, options ...SerializeOption) {
	out.WriteString("LATERAL")
	s.selectStmt.serialize(statement, out)

	out.WriteString("AS")
	out.WriteIdentifier(s.alias)
}
