package sqlbuilder

type rowsType interface {
	clause
	projections() []projection
}
