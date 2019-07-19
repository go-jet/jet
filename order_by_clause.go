package jet

import "errors"

// OrderByClause
type orderByClause interface {
	serializeForOrderBy(statement statementType, out *sqlBuilder) error
}

type orderByClauseImpl struct {
	expression Expression
	ascent     bool
}

func (o *orderByClauseImpl) serializeForOrderBy(statement statementType, out *sqlBuilder) error {
	if o.expression == nil {
		return errors.New("jet: nil orderBy by clause")
	}

	if err := o.expression.serializeForOrderBy(statement, out); err != nil {
		return err
	}

	if o.ascent {
		out.writeString("ASC")
	} else {
		out.writeString("DESC")
	}

	return nil
}

func newOrderByClause(expression Expression, ascent bool) orderByClause {
	return &orderByClauseImpl{expression: expression, ascent: ascent}
}