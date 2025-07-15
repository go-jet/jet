package postgres

import "github.com/go-jet/jet/v2/internal/jet"

func COLLATE(exp StringExpression, collation string) StringExpression {
	return StringExp(CustomExpression(exp, jet.Raw("COLLATE"), jet.Keyword(collation)))
}
