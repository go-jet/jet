package sqlbuilder

// A clause that can be used in order by
type OrderByClause interface {
	Clause
	isOrderByClauseInterface
}

// A clause that is selectable.
//type Projection interface {
//	Clause
//	isProjectionInterface
//
//	SerializeSqlForColumnList(out *bytes.Buffer) error
//}

//type ColumnList []Column
//
//func (cl ColumnList) SerializeSql(out *bytes.Buffer, options ...serializeOption) error {
//	for i, column := range cl {
//		column.SerializeSql(out)
//
//		if i != len(cl)-1 {
//			out.WriteString(", ")
//		}
//	}
//	return nil
//}
//
//func (cl ColumnList) isProjectionType() {
//}
//
//func (cl ColumnList) As(name string) Clause {
//	panic("Unallowed operation ")
//}

//func (cl ColumnList) SerializeSqlForColumnList(out *bytes.Buffer) error {
//	for i, column := range cl {
//		column.SerializeSqlForColumnList(out)
//
//		if i != len(cl)-1 {
//			out.WriteString(", ")
//		}
//	}
//	return nil
//}

//
// Boiler plates ...
//

type isOrderByClauseInterface interface {
	isOrderByClauseType()
}

type isOrderByClause struct {
}

func (o *isOrderByClause) isOrderByClauseType() {
}

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
