
Jet sql builder supports following expression types:

 - Bool expressions
 - Integer expressions
 - Float expressions
 - String expressions
 - Date expressions
 - Time expressions
 - Timez expressions (with time zone)
 - Timestamp expressions
 - Timestampz expressions (with time zone)
 
_This list might be extended with feature Jet releases._  

### Literal type
For every expression type there is a method to create one literal type expressions.  
Literal type examples:

```
Bool(true)  
Integer(11)  
Float(23.44)  
String("John Doe")  
Date(2010, 12, 3)  
Time(23, 6, 6, 1)  
Timez(23, 6, 6, 222, +200)  
Timestamp(2010, 10, 21, 15, 30, 12, 333)  
Timestampz(2010, 10, 21, 15, 30, 12, 444, 0)

NULL
STAR (alias for *)  
```

### Column types
Every sql builder table column belongs to one expression type. There are following column types:
```
ColumnBool
ColumnInteger
ColumnFloat
ColumnString
ColumnDate
ColumnTime
ColumnTimez
ColumnTimestamp
ColumnTimestampz
```

Columns and literals can form arbitrary expressions but have to follow valid SQL expression syntax.
For instance valid expressions are:

```
Bool(true).AND(Bool(false)).IS_FALSE()
(Film.Length.GT(Int(100)).AND(Film.Length.LT(Int(200))).IS_TRUE()          // table 'film', integer column 'length'
```

Some of the invalid expressions. These expressions will cause go build to break.

```
Bool(true).AND(Int(11))        // can't compare bool and
Int(11).ADD(Float(22.2))       // can't add integer and floats, but 
                               // using cast it is possible: Int(11).TO_FLOAT().ADD(Float(22.2))
```

## Common expression operators

```go
type Expression interface {
    clause
    projection
    groupByClause
    OrderByClause
    // Test expression whether it is a NULL value.
    IS_NULL() BoolExpression
    // Test expression whether it is a non-NULL value.
    IS_NOT_NULL() BoolExpression
    
    // Check if this expressions matches any in expressions list
    IN(expressions ...Expression) BoolExpression
    // Check if this expressions is different of all expressions in expressions list
    NOT_IN(expressions ...Expression) BoolExpression
    
    // The temporary alias name to assign to the expression
    AS(alias string) projection
    
    // Expression will be used to sort query result in ascending order
    ASC() OrderByClause
    // Expression will be used to sort query result in ascending order
    DESC() OrderByClause
    
    // Cast expression to dbType
    TO(dbType string) Expression
    // Cast expression to bool type
    TO_BOOL() BoolExpression
    // Cast expression to smallint type
    TO_SMALLINT() IntegerExpression
    // Cast expression to integer type
    TO_INTEGER() IntegerExpression
    // Cast expression to bigint type
    TO_BIGINT() IntegerExpression
    // Cast expression to numeric type, using precision and optionally scale
    TO_NUMERIC(precision int, scale ...int) FloatExpression
    // Cast expression to real type
    TO_REAL() FloatExpression
    // Cast expression to double precision type
    TO_DOUBLE() FloatExpression
    // Cast expression to text type
    TO_TEXT() StringExpression
    // Cast expression to date type
    TO_DATE() DateExpression
    // Cast expression to time type
    TO_TIME() TimeExpression
    // Cast expression to time with time timezone type
    TO_TIMEZ() TimezExpression
    // Cast expression to timestamp type
    TO_TIMESTAMP() TimestampExpression
    // Cast expression to timestamp with timezone type
    TO_TIMESTAMPZ() TimestampzExpression
}
```
Examples:
```go
Film.Description.IS_NULL()
Int(1).ADD(Int(2)).AS("1+2")
(Film.Duration.ADD(Int(3))).ASC()
String("1999-01-08 13:05:06 +0100 CET").TO_TIMESTAMPZ()
```

### Bool expression operators:
```go
type BoolExpression interface {
	Expression

	// Check if this expression is equal to rhs
	EQ(rhs BoolExpression) BoolExpression
	// Check if this expression is not equal to rhs
	NOT_EQ(rhs BoolExpression) BoolExpression
	// Check if this expression is distinct to rhs
	IS_DISTINCT_FROM(rhs BoolExpression) BoolExpression
	// Check if this expression is not distinct to rhs
	IS_NOT_DISTINCT_FROM(rhs BoolExpression) BoolExpression

	// Check if this expression is true
	IS_TRUE() BoolExpression
	// Check if this expression is not true
	IS_NOT_TRUE() BoolExpression
	// Check if this expression is false
	IS_FALSE() BoolExpression
	// Check if this expression is not false
	IS_NOT_FALSE() BoolExpression
	// Check if this expression is unknown
	IS_UNKNOWN() BoolExpression
	// Check if this expression is not unknown
	IS_NOT_UNKNOWN() BoolExpression

	// expression AND operator rhs
	AND(rhs BoolExpression) BoolExpression
	// expression OR operator rhs
	OR(rhs BoolExpression) BoolExpression
}
```

Examples:
```
Staff.Active.EQ(Bool(true))
Staff.Active.IS_TRUE()

Bool(true).AND(Staff.Active).OR(Bool(false))
```

### Integer expression operators

```go
type IntegerExpression interface {
	Expression
	numericExpression

	// Check if expression is equal to rhs
	EQ(rhs IntegerExpression) BoolExpression
	// Check if expression is not equal to rhs
	NOT_EQ(rhs IntegerExpression) BoolExpression
	// Check if expression is distinct from rhs
	IS_DISTINCT_FROM(rhs IntegerExpression) BoolExpression
	// Check if expression is not distinct from rhs
	IS_NOT_DISTINCT_FROM(rhs IntegerExpression) BoolExpression

	// Check if expression is less then rhs
	LT(rhs IntegerExpression) BoolExpression
	// Check if expression is less then equal rhs
	LT_EQ(rhs IntegerExpression) BoolExpression
	// Check if expression is greater then rhs
	GT(rhs IntegerExpression) BoolExpression
	// Check if expression is greater then equal rhs
	GT_EQ(rhs IntegerExpression) BoolExpression

	// expression + rhs
	ADD(rhs IntegerExpression) IntegerExpression
	// expression - rhs
	SUB(rhs IntegerExpression) IntegerExpression
	// expression * rhs
	MUL(rhs IntegerExpression) IntegerExpression
	// expression / rhs
	DIV(rhs IntegerExpression) IntegerExpression
	// expression % rhs
	MOD(rhs IntegerExpression) IntegerExpression
	// expression ^ rhs
	POW(rhs IntegerExpression) IntegerExpression

	// expression & rhs
	BIT_AND(rhs IntegerExpression) IntegerExpression
	// expression | rhs
	BIT_OR(rhs IntegerExpression) IntegerExpression
	// expression # rhs
	BIT_XOR(rhs IntegerExpression) IntegerExpression
	// ~expression
	BIT_NOT() IntegerExpression
	// expression << rhs
	BIT_SHIFT_LEFT(shift IntegerExpression) IntegerExpression
	// expression >> rhs
	BIT_SHIFT_RIGHT(shift IntegerExpression) IntegerExpression
}
```

Examples:
```go
(Film.Length.ADD(Int(20))).LT(Int(200))
Film.LanguageID.EQ(Int(2))
Film.LanguageID.BIT_SHIFT_LEFT(Int(2))
```