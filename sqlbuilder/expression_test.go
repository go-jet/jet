package sqlbuilder

import (
	"gotest.tools/assert"
	"testing"
)

func TestExpressionIS_NULL(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table2Col3.IS_NULL()), "table2.col3 IS NULL")
	assert.Equal(t, getTestSerialize(t, table2Col3.ADD(table2Col3).IS_NULL()), "(table2.col3 + table2.col3) IS NULL")
}

func TestExpressionIS_NOT_NULL(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table2Col3.IS_NOT_NULL()), "table2.col3 IS NOT NULL")
	assert.Equal(t, getTestSerialize(t, table2Col3.ADD(table2Col3).IS_NOT_NULL()), "(table2.col3 + table2.col3) IS NOT NULL")
}

func TestExpressionIS_DISTINCT_FROM(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table2Col3.IS_DISTINCT_FROM(table2Col4)), "(table2.col3 IS DISTINCT FROM table2.col4)")
	assert.Equal(t, getTestSerialize(t, table2Col3.ADD(table2Col3).IS_DISTINCT_FROM(Int(23))), "((table2.col3 + table2.col3) IS DISTINCT FROM $1)")
}

func TestExpressionIS_NOT_DISTINCT_FROM(t *testing.T) {
	assert.Equal(t, getTestSerialize(t, table2Col3.IS_NOT_DISTINCT_FROM(table2Col4)), "(table2.col3 IS NOT DISTINCT FROM table2.col4)")
	assert.Equal(t, getTestSerialize(t, table2Col3.ADD(table2Col3).IS_NOT_DISTINCT_FROM(Int(23))), "((table2.col3 + table2.col3) IS NOT DISTINCT FROM $1)")
}
