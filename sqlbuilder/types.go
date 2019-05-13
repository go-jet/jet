package sqlbuilder

type rowsType interface {
	clause
	hasRows()
}

type isRowsType struct{}

func (i *isRowsType) hasRows() {}

// A clause that can be used in orderBy by

// A clause that is selectable.
//type projection interface {
//	clause
//	isProjectionInterface
//
//	SerializeSqlForColumnList(out *bytes.Buffer) error
//}

//
// Boiler plates ...
//

//
//type isProjectionInterface interface {
//	isProjectionType()
//}
//
//type isProjection struct {
//}
//
//func (p *isProjection) isProjectionType() {
//}
