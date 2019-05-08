package sqlbuilder

import "github.com/dropbox/godropbox/errors"

type orderByClause interface {
	serializeAsOrderBy(statement statementType, out *queryData) error
}

type orderByClauseImpl struct {
	expression expression
	ascent     bool
}

func (o *orderByClauseImpl) serializeAsOrderBy(statement statementType, out *queryData) error {
	if o.expression == nil {
		return errors.Newf("nil orderBy by clause.")
	}

	if err := o.expression.serializeAsOrderBy(statement, out); err != nil {
		return err
	}

	if o.ascent {
		out.writeString(" ASC")
	} else {
		out.writeString(" DESC")
	}

	return nil
}

func ASC(expression expression) orderByClause {
	return &orderByClauseImpl{expression: expression, ascent: true}
}

func DESC(expression expression) orderByClause {
	return &orderByClauseImpl{expression: expression, ascent: false}
}
