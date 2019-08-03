package jet

import "errors"

// OrderByClause
type orderByClause interface {
	serializeForOrderBy(statement StatementType, out *SqlBuilder) error
}

type orderByClauseImpl struct {
	expression Expression
	ascent     bool
}

func (o *orderByClauseImpl) serializeForOrderBy(statement StatementType, out *SqlBuilder) error {
	if o.expression == nil {
		return errors.New("jet: nil orderBy by clause")
	}

	if err := o.expression.serializeForOrderBy(statement, out); err != nil {
		return err
	}

	if o.ascent {
		out.WriteString("ASC")
	} else {
		out.WriteString("DESC")
	}

	return nil
}

func newOrderByClause(expression Expression, ascent bool) orderByClause {
	return &orderByClauseImpl{expression: expression, ascent: ascent}
}
