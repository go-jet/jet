package sqlbuilder

import "github.com/dropbox/godropbox/errors"

type orderByClause interface {
	clause
	isOrderByClauseType()
}

type isOrderByClause struct {
}

func (o *isOrderByClause) isOrderByClauseType() {
}

type orderByClauseImpl struct {
	isOrderByClause
	expression expression
	ascent     bool
}

func (o *orderByClauseImpl) serialize(out *queryData) error {
	if o.expression == nil {
		return errors.Newf("nil orderBy by clause.")
	}

	if err := o.expression.serialize(out); err != nil {
		return err
	}

	if o.ascent {
		out.WriteString(" ASC")
	} else {
		out.WriteString(" DESC")
	}

	return nil
}

func ASC(expression expression) orderByClause {
	return &orderByClauseImpl{expression: expression, ascent: true}
}

func DESC(expression expression) orderByClause {
	return &orderByClauseImpl{expression: expression, ascent: false}
}
