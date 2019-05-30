package sqlbuilder

//----------- Logical operators ---------------//

// Returns a representation of "not expr"
func NOT(expr boolExpression) boolExpression {
	return newPrefixBoolExpression(expr, "NOT")
}

//----------- Comparison operators ---------------//

// Returns a representation of "a=b"
func EQ(lhs, rhs expression) boolExpression {
	return newBinaryBoolExpression(lhs, rhs, "=")
}

// Returns a representation of "a!=b"
func NOT_EQ(lhs, rhs expression) boolExpression {
	return newBinaryBoolExpression(lhs, rhs, "!=")
}

func IS_DISTINCT_FROM(lhs, rhs expression) boolExpression {
	return newBinaryBoolExpression(lhs, rhs, "IS DISTINCT FROM")
}

func IS_NOT_DISTINCT_FROM(lhs, rhs expression) boolExpression {
	return newBinaryBoolExpression(lhs, rhs, "IS NOT DISTINCT FROM")
}

// Returns a representation of "a<b"
func LT(lhs expression, rhs expression) boolExpression {
	return newBinaryBoolExpression(lhs, rhs, "<")
}

// Returns a representation of "a<=b"
func LT_EQ(lhs, rhs expression) boolExpression {
	return newBinaryBoolExpression(lhs, rhs, "<=")
}

// Returns a representation of "a>b"
func GT(lhs, rhs expression) boolExpression {
	return newBinaryBoolExpression(lhs, rhs, ">")
}

// Returns a representation of "a>=b"
func GT_EQ(lhs, rhs expression) boolExpression {
	return newBinaryBoolExpression(lhs, rhs, ">=")
}

func IS_TRUE(expr boolExpression) boolExpression {
	return newPostifxBoolExpression(expr, "IS TRUE")
}

func IS_NOT_TRUE(expr boolExpression) boolExpression {
	return newPostifxBoolExpression(expr, "IS NOT TRUE")
}

func IS_FALSE(expr boolExpression) boolExpression {
	return newPostifxBoolExpression(expr, "IS FALSE")
}

func IS_NOT_FALSE(expr boolExpression) boolExpression {
	return newPostifxBoolExpression(expr, "IS NOT FALSE")
}

func IS_UNKNOWN(expr boolExpression) boolExpression {
	return newPostifxBoolExpression(expr, "IS UNKNOWN")
}

func IS_NOT_UNKNOWN(expr boolExpression) boolExpression {
	return newPostifxBoolExpression(expr, "IS NOT UNKNOWN")
}

func And(lhs, rhs expression) boolExpression {
	return newBinaryBoolExpression(lhs, rhs, "AND")
}

// Returns a representation of "c[0] OR ... OR c[n-1]" for c in clauses
func Or(lhs, rhs expression) boolExpression {
	return newBinaryBoolExpression(lhs, rhs, "OR")
}

func Like(lhs, rhs expression) boolExpression {
	return newBinaryBoolExpression(lhs, rhs, "LIKE")
}

func LikeL(lhs expression, val string) boolExpression {
	return Like(lhs, Literal(val))
}

func Regexp(lhs, rhs expression) boolExpression {
	return newBinaryBoolExpression(lhs, rhs, "REGEXP")
}

func RegexpL(lhs expression, val string) boolExpression {
	return Regexp(lhs, Literal(val))
}

func EXISTS(subQuery selectStatement) boolExpression {
	return newPrefixBoolExpression(subQuery, "EXISTS")
}
