package jet

type rowsType interface {
	clause
	projections() []projection
}
