package sqlbuilder

import "errors"

type OrderByClause interface {
	serializeForOrderBy(statement statementType, out *queryData) error
}

type orderByClauseImpl struct {
	expression Expression
	ascent     bool
}

func (o *orderByClauseImpl) serializeForOrderBy(statement statementType, out *queryData) error {
	if o.expression == nil {
		return errors.New("nil orderBy by clause.")
	}

	if err := o.expression.serializeForOrderBy(statement, out); err != nil {
		return err
	}

	if o.ascent {
		out.writeString(" ASC")
	} else {
		out.writeString(" DESC")
	}

	return nil
}

func ASC(expression Expression) OrderByClause {
	return &orderByClauseImpl{expression: expression, ascent: true}
}

func DESC(expression Expression) OrderByClause {
	return &orderByClauseImpl{expression: expression, ascent: false}
}
