package jet

// GroupByClause interface
type GroupByClause interface {
	serializeForGroupBy(statement StatementType, out *SQLBuilder)
}

// GROUPING_SETS operator allows grouping of the rows in a table by multiple sets of columns in a single query.
// This can be useful when we want to analyze data by different combinations of columns, without having to write separate
// queries for each combination.
func GROUPING_SETS(expressions ...Expression) GroupByClause {
	return Func("GROUPING SETS", expressions...)
}

// ROLLUP operator is used with the GROUP BY clause to generate all prefixes of a group of columns including the empty list.
// It creates extra rows in the result set that represent the subtotal values for each combination of columns.
func ROLLUP(expressions ...Expression) GroupByClause {
	return Func("ROLLUP", expressions...)
}

// CUBE operator is used with the GROUP BY clause to generate subtotals for all possible combinations of a group of columns.
// It creates extra rows in the result set that represent the subtotal values for each combination of columns.
func CUBE(expressions ...Expression) GroupByClause {
	return Func("CUBE", expressions...)
}

// GROUPING function is used to identify which columns are included in a grouping set or a subtotal row. It takes as input
// the name of a column and returns 1 if the column is not included in the current grouping set, and 0 otherwise.
// It can be also used with multiple parameters to check if a set of columns is included in the current grouping set. The result
// of the GROUPING function would then be an integer bit mask having 1’s for the arguments which have GROUPING(argument) as 1.
func GROUPING(expressions ...Expression) IntegerExpression {
	return IntExp(Func("GROUPING", expressions...))
}

// WITH_ROLLUP operator is used with the GROUP BY clause to generate all prefixes of a group of columns including the empty list.
// It creates extra rows in the result set that represent the subtotal values for each combination of columns.
func WITH_ROLLUP(expressions ...Expression) GroupByClause {
	return CustomExpression(
		parametersSerializer(expressions), Token("WITH ROLLUP"),
	)
}
