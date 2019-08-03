package postgres

import "github.com/go-jet/jet/internal/jet"

type SelectStatement jet.SelectStatement

var SELECT = jet.SELECT

func UNION(lhs, rhs SelectStatement, selects ...SelectStatement) SelectStatement {
	return jet.UNION(lhs, rhs, toJetSelects(selects...)...)
}

func UNION_ALL(lhs, rhs SelectStatement, selects ...SelectStatement) SelectStatement {
	return jet.UNION_ALL(lhs, rhs, toJetSelects(selects...)...)
}

func INTERSECT(lhs, rhs SelectStatement, selects ...SelectStatement) SelectStatement {
	return jet.INTERSECT(lhs, rhs, toJetSelects(selects...)...)
}

func INTERSECT_ALL(lhs, rhs SelectStatement, selects ...SelectStatement) SelectStatement {
	return jet.INTERSECT_ALL(lhs, rhs, toJetSelects(selects...)...)
}

func toJetSelects(selects ...SelectStatement) []jet.SelectStatement {
	ret := []jet.SelectStatement{}

	for _, sel := range selects {
		ret = append(ret, sel)
	}

	return ret
}

type SelectLock jet.SelectLock

var (
	UPDATE        = jet.NewSelectLock("UPDATE")
	NO_KEY_UPDATE = jet.NewSelectLock("NO KEY UPDATE")
	SHARE         = jet.NewSelectLock("SHARE")
	KEY_SHARE     = jet.NewSelectLock("KEY SHARE")
)
