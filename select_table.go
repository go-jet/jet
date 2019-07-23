package jet

import "errors"

// SelectTable is interface for SELECT sub-queries
type SelectTable interface {
	ReadableTable

	Alias() string

	AllColumns() ProjectionList
}

type selectTableImpl struct {
	readableTableInterfaceImpl
	selectStmt SelectStatement
	alias      string

	projections []projection
}

func newSelectTable(selectStmt SelectStatement, alias string) SelectTable {
	expTable := &selectTableImpl{selectStmt: selectStmt, alias: alias}

	expTable.readableTableInterfaceImpl.parent = expTable

	for _, projection := range selectStmt.projections() {
		newProjection := projection.from(expTable)

		expTable.projections = append(expTable.projections, newProjection)
	}

	return expTable
}

func (s *selectTableImpl) Alias() string {
	return s.alias
}

func (s *selectTableImpl) columns() []column {
	return nil
}

func (s *selectTableImpl) AllColumns() ProjectionList {
	return s.projections
}

func (s *selectTableImpl) serialize(statement statementType, out *sqlBuilder, options ...serializeOption) error {
	if s == nil {
		return errors.New("jet: Expression table is nil. ")
	}

	err := s.selectStmt.serialize(statement, out)

	if err != nil {
		return err
	}

	out.writeString("AS")
	out.writeIdentifier(s.alias)

	return nil
}
