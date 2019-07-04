
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
For every expression type there is a method to create one expression literal type .  
Literal type examples:

```
jet.Bool(true)  
jet.Integer(11)  
jet.Float(23.44)  
jet.String("John Doe")  
jet.Date(2010, 12, 3)  
jet.Time(23, 6, 6, 1)  
jet.Timez(23, 6, 6, 222, +200)  
jet.Timestamp(2010, 10, 21, 15, 30, 12, 333)  
jet.Timestampz(2010, 10, 21, 15, 30, 12, 444, 0)

jet.NULL
jet.STAR (alias for *)  
```

### Column types
Every sql builder table column belongs to one expression type. There are following column types:
```
jet.ColumnBool
jet.ColumnInteger
jet.ColumnFloat
jet.ColumnString
jet.ColumnDate
jet.ColumnTime
jet.ColumnTimez
jet.ColumnTimestamp
jet.ColumnTimestampz
```

Columns and literals can form arbitrary expressions but have to follow valid SQL expression syntax.
For instance valid expressions are:

```
jet.Bool(true).AND(jet.Bool(false)).IS_FALSE()
(table.Film.Length.GT(jet.Int(100)).AND(table.Film.Length.LT(jet.Int(200))).IS_TRUE()         
```

Some of the invalid expressions. These expressions will cause go build to break.

```
jet.Bool(true).ADD(jet.Int(11))        // can't add bool and integer
jet.Int(11).LIKE(jet.Float(22.2))      // integer expressions doesn't have LIKE method
 ```


## Comparision operators

Jet supports following comparison operators for all expression types:

| Method                         | Example                                             | Generated sql                        |
| ------------------------------ | ------------------------------------------------   |----------------------------          |
| EQ                             | jet.Int(1).EQ(table.Film.Length)                   | 1 = film.length                      |
| NOT_EQ                         | jet.Int(1).EQ(table.Film.Length)                   | 1 != film.length                     |
| IS_DISTINCT_FROM               | jet.Int(1).IS_DISTINCT_FROM(table.Film.Length)     | 1 IS DISTINCT FROM film.length       |
| IS_NOT_DISTINCT_FROM           | jet.Int(1).IS_NOT_DISTINCT_FROM(table.Film.Length) | 1 IS NOT DISTINCT FROM film.length   |
| LT                             | jet.Int(1).LT(table.Film.Length)                   | 1 < film.length                      |
| LT_EQ                          | jet.Int(1).LT_EQ(table.Film.Length)                | 1 <= film.length                     |
| GT                             | jet.Int(1).GT(table.Film.Length)                   | 1 > film.length                      |
| GT_EQ                          | jet.Int(1).GT_EQ(table.Film.Length)                | 1 >= film.length                     |

*Left-hand side and right-hand side of operators have to be of the same type*


## Arithmetic operators

Following arithmetic operators are supported for integer and float expressions. 
If the first argument is float expression, second argument can be integer or float expression.
If the first argument is integer expression second argument can only be integer expression.

| Method                         | Example                                             | Generated sql                        |
| ------------------------------ | ------------------------------------------------   |----------------------------          |
| ADD                            | jet.Int(1).ADD(table.Film.Length)                  | 1 + film.length                      |
| SUB                            | jet.Float(1.11).SUB(Int(1))                        | 1.11 + 1                             |
| MUL                            | jet.Int(1).MUL(table.Film.Length)                  | 1 * film.length                      |
| DIV                            | jet.Float(1.11).DIV(jet.Float(3.33)                | 1.11 / 3.33                          |
| MOD                            | jet.Int(10).MOD(table.Film.Length)                 | 10 % film.length                     |
| POW                            | jet.Float(10.01).POW(table.Film.Length)            | 10.01 ^ film.length                  |


## Bit operators

Following operators are only available on integer expressions:

| Method                         | Example                                             | Generated sql                        |
| ------------------------------ | ------------------------------------------------   |----------------------------          |
| BIT_AND                        | jet.Int(11).BIT_AND(table.Film.Length)             | 11 & film.length                     |
| BIT_OR                         | jet.Int(11).BIT_OR(table.Film.Length)              | 11 \| film.length                    |
| BIT_XOR                        | jet.Int(11).BIT_XOR(table.Film.Length)             | 11 # film.length                     |
| BIT_NOT                        | jet.Int(11).BIT_NOT(table.Film.Length)             | ~ 11                                 |
| BIT_SHIFT_LEFT                 | jet.Int(11).BIT_SHIFT_LEFT(table.Film.Length)      | 11 >> film.length                    |
| BIT_SHIFT_RIGHT                | jet.Int(11).BIT_SHIFT_RIGHT(table.Film.Length)     | 11 >> film.length                    |


## Logical operators

Following operators are only available on boolean expressions:

| Method                         | Example                                                   | Generated sql                        |
| ------------------------------ | ---------------------------------------------------------|----------------------------          |
| IS_TRUE                        | table.Staff.Active.IS_TRUE()                             | staff.active IS TRUE                 |
| IS_NOT_TRUE                    | (table.Staff.Active.AND(jet.Bool(true))).IS_NOT_TRUE()   | (staff.active AND true) IS NOT TRUE  |
| IS_FALSE                       | jet.Bool(false).IS_FALSE()                               | false IS FALSE                       |
| IS_NOT_FALSE                   | jet.Bool(true).IS_NOT_FALSE()                            | true IS NOT FALSE                    |
| IS_UNKNOWN                     | table.Staff.Active.IS_UNKNOWN()                          | staff.active IS UNKNOWN              |
| IS_NOT_UNKNOWN                 | table.Staff.Active.IS_NOT_UNKNOWN()                      | staff.active IS NOT UNKNOWN          |


## String operators

Following operators are only available on string expressions:

| Method                         | Example                                                   | Generated sql                        |
| ------------------------------ | ---------------------------------------------------------|----------------------------          |
| CONCAT                         | table.Film.Name.CONCAT(table.Film.Description)           | film.name \|\| film.description      |
| LIKE                           | table.Film.Name.LIKE(String("%Wind%"))                   | film.name LIKE %Wind%                |
| NOT_LIKE                       | table.Film.Name.NOT_LIKE(String("%Wind%"))               | staff.active NOT LIKE %Wind%         |
| SIMILAR_TO                     | table.Film.Name.SIMILAR_TO(String("%Wind%"))             | staff.active SIMILAR TO %Wind%       |
| NOT_SIMILAR_TO                 | table.Film.Name.NOT_SIMILAR_TO(String("%Wind%"))         | staff.active NOT SIMILAR TO %Wind%   |


## SQL Cast operators

Cast operators allow expressions to be casted to some other database type.
SQL builder expression type changes accordingly to database type.

| Method                         | Example                                     | Generated sql                   |
| ------------------------------ | -------------------------------------------|----------------------------     |
| TO_BOOL                        | table.Film.Description.TO_BOOL()           | film.description::boolean       |
| TO_SMALLINT                    | table.Film.Description.TO_SMALLINT()       | film.description::smallint      |
| TO_INTEGER                     | table.Film.Description.TO_INTEGER()        | film.description::integer       |
| TO_BIGINT                      | table.Film.Description.TO_BIGINT()         | film.description::bigint        |
| TO_NUMERIC                     | table.Film.Description.TO_NUMERIC(10, 6)   | film.description::numeric(10,6) |
| TO_REAL                        | table.Film.Description.TO_REAL()           | film.description::real          |
| TO_DOUBLE                      | table.Film.Description.TO_DOUBLE()         | film.description::double        |
| TO_TEXT                        | table.Film.Description.TO_TEXT()           | film.description::text          |
| TO_DATE                        | table.Film.Description.TO_DATE()           | film.description::date          |
| TO_TIME                        | table.Film.Description.TO_TIME()           | film.description::time          |
| TO_TIMEZ                       | table.Film.Description.TO_TIMEZ()          | film.description::timez         |
| TO_TIMESTAMP                   | table.Film.Description.TO_TIMESTAMP()      | film.description::timestamp     |
| TO_TIMESTAMPZ                  | table.Film.Description.TO_TIMESTAMPZ()     | film.description::timestampz    |

## SQL builder cast

TODO:
