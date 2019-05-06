package sqlbuilder

import "github.com/dropbox/godropbox/errors"

type OrderByClause interface {
	Clause
	isOrderByClauseType()
}

type isOrderByClause struct {
}

func (o *isOrderByClause) isOrderByClauseType() {
}

type ColumnNameOrderBy string

func (o *ColumnNameOrderBy) isOrderByClauseType() {
}

func (o *ColumnNameOrderBy) Serialize(out *queryData, options ...serializeOption) error {
	return nil
}

type orderByClause struct {
	isOrderByClause
	expression Expression
	ascent     bool
}

func (o *orderByClause) Serialize(out *queryData, options ...serializeOption) error {
	if o.expression == nil {
		return errors.Newf("nil orderBy by clause.")
	}

	if err := o.expression.Serialize(out); err != nil {
		return err
	}

	if o.ascent {
		out.WriteString(" ASC")
	} else {
		out.WriteString(" DESC")
	}

	return nil
}

func Asc(expression Expression) OrderByClause {
	return &orderByClause{expression: expression, ascent: true}
}

func Desc(expression Expression) OrderByClause {
	return &orderByClause{expression: expression, ascent: false}
}
