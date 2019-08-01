package postgres

import (
	"github.com/go-jet/jet"
	"testing"
)

var dateVar = Date(2000, 12, 30)

func TestDateExpressionEQ(t *testing.T) {
	jet.AssertPostgreClauseSerialize(t, table1ColDate.EQ(table2ColDate), "(table1.col_date = table2.col_date)")
	jet.AssertPostgreClauseSerialize(t, table1ColDate.EQ(dateVar), "(table1.col_date = $1::DATE)", "2000-12-30")
}

func TestDateExpressionNOT_EQ(t *testing.T) {
	jet.AssertPostgreClauseSerialize(t, table1ColDate.NOT_EQ(table2ColDate), "(table1.col_date != table2.col_date)")
	jet.AssertPostgreClauseSerialize(t, table1ColDate.NOT_EQ(dateVar), "(table1.col_date != $1::DATE)", "2000-12-30")
}

func TestDateExpressionIS_DISTINCT_FROM(t *testing.T) {
	jet.AssertPostgreClauseSerialize(t, table1ColDate.IS_DISTINCT_FROM(table2ColDate), "(table1.col_date IS DISTINCT FROM table2.col_date)")
	jet.AssertPostgreClauseSerialize(t, table1ColDate.IS_DISTINCT_FROM(dateVar), "(table1.col_date IS DISTINCT FROM $1::DATE)", "2000-12-30")
}

func TestDateExpressionIS_NOT_DISTINCT_FROM(t *testing.T) {
	jet.AssertPostgreClauseSerialize(t, table1ColDate.IS_NOT_DISTINCT_FROM(table2ColDate), "(table1.col_date IS NOT DISTINCT FROM table2.col_date)")
	jet.AssertPostgreClauseSerialize(t, table1ColDate.IS_NOT_DISTINCT_FROM(dateVar), "(table1.col_date IS NOT DISTINCT FROM $1::DATE)", "2000-12-30")
}

func TestDateExpressionGT(t *testing.T) {
	jet.AssertPostgreClauseSerialize(t, table1ColDate.GT(table2ColDate), "(table1.col_date > table2.col_date)")
	jet.AssertPostgreClauseSerialize(t, table1ColDate.GT(dateVar), "(table1.col_date > $1::DATE)", "2000-12-30")
}

func TestDateExpressionGT_EQ(t *testing.T) {
	jet.AssertPostgreClauseSerialize(t, table1ColDate.GT_EQ(table2ColDate), "(table1.col_date >= table2.col_date)")
	jet.AssertPostgreClauseSerialize(t, table1ColDate.GT_EQ(dateVar), "(table1.col_date >= $1::DATE)", "2000-12-30")
}

func TestDateExpressionLT(t *testing.T) {
	jet.AssertPostgreClauseSerialize(t, table1ColDate.LT(table2ColDate), "(table1.col_date < table2.col_date)")
	jet.AssertPostgreClauseSerialize(t, table1ColDate.LT(dateVar), "(table1.col_date < $1::DATE)", "2000-12-30")
}

func TestDateExpressionLT_EQ(t *testing.T) {
	jet.AssertPostgreClauseSerialize(t, table1ColDate.LT_EQ(table2ColDate), "(table1.col_date <= table2.col_date)")
	jet.AssertPostgreClauseSerialize(t, table1ColDate.LT_EQ(dateVar), "(table1.col_date <= $1::DATE)", "2000-12-30")
}
