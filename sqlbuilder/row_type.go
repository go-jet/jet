package sqlbuilder

type rowsType interface {
	clause
	hasRows()
}

type isRowsType struct{}

func (i *isRowsType) hasRows() {}
