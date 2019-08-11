package jet

//// SelectTable is interface for SELECT sub-queries
//type SelectTable interface {
//	ReadableTable
//
//	Alias() string
//
//	AllColumns() ProjectionList
//}
//
//type selectTableImpl struct {
//	readableTableInterfaceImpl
//	selectStmt SelectStatement
//	alias      string
//
//	projections []Projection
//}
//
//func newSelectTable(selectStmt SelectStatement, alias string) SelectTable {
//	expTable := &selectTableImpl{selectStmt: selectStmt, alias: alias}
//
//	expTable.readableTableInterfaceImpl.parent = expTable
//
//	for _, projection := range selectStmt.projections() {
//		newProjection := projection.fromImpl(expTable)
//
//		expTable.projections = append(expTable.projections, newProjection)
//	}
//
//	return expTable
//}
//
//func (s *selectTableImpl) Alias() string {
//	return s.alias
//}
//
//func (s *selectTableImpl) columns() []IColumn {
//	return nil
//}
//
//func (s *selectTableImpl) accept(visitor visitor) {
//	visitor.visit(s)
//	s.selectStmt.accept(visitor)
//}
//
//func (s *selectTableImpl) dialect() Dialect {
//	return detectDialect(s.selectStmt)
//}
//
//func (s *selectTableImpl) AllColumns() ProjectionList {
//	return s.projections
//}
//
//func (s *selectTableImpl) serialize(statement StatementType, out *SqlBuilder, options ...SerializeOption) error {
//	if s == nil {
//		return errors.New("jet: Expression table is nil. ")
//	}
//
//	err := s.selectStmt.serialize(statement, out)
//
//	if err != nil {
//		return err
//	}
//
//	out.WriteString("AS")
//	out.writeIdentifier(s.alias)
//
//	return nil
//}
