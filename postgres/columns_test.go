package postgres

import (
	"testing"
)

func TestNewIntervalColumn(t *testing.T) {
	subQuery := SELECT(Int(1)).AsTable("sub_query")

	subQueryIntervalColumn := IntervalColumn("col_interval").From(subQuery)
	assertSerialize(t, subQueryIntervalColumn, `sub_query.col_interval`)
	assertSerialize(t, subQueryIntervalColumn.EQ(INTERVAL(2, HOUR, 10, MINUTE)),
		`(sub_query.col_interval = INTERVAL '2 HOUR 10 MINUTE')`)
	assertProjectionSerialize(t, subQueryIntervalColumn, `sub_query.col_interval AS "col_interval"`)

	subQueryIntervalColumn2 := table1ColInterval.From(subQuery)
	assertSerialize(t, subQueryIntervalColumn2, `sub_query."table1.col_interval"`)
	assertSerialize(t, subQueryIntervalColumn2.EQ(INTERVAL(1, DAY)), `(sub_query."table1.col_interval" = INTERVAL '1 DAY')`)
	assertProjectionSerialize(t, subQueryIntervalColumn2, `sub_query."table1.col_interval" AS "table1.col_interval"`)
}
