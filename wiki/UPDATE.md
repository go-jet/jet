UPDATE changes the values of the specified columns in all rows that satisfy the condition.
More about UPDATE statement in PostgreSQL: https://www.postgresql.org/docs/11/sql-update.html

Following clauses are supported:
- UPDATE(columns...) - list of columns to update
- SET(values...) - list of values for columns
- MODEL(model) - list of values for columns will be extracted from model object
- WHERE(condition) - only rows for which condition returns true will be updated.
- RETURNING(output_expression...) - An expressions to be computed and returned by the UPDATE statement after each row is updated.
The expressions can use any column names of the table. Write _TableName_.AllColumns to return all columns.

_This list might be extended with feature Jet releases._ 

## Example

```
// replace all Bing links with Yahoo
updateStmt := Link.
    UPDATE(Link.Name, Link.URL).
    SET("Yahoo", "http://yahoo.com").
    WHERE(Link.Name.EQ(String("Bing")))
```

Debug sql of above statement:
```sql
UPDATE test_sample.link          -- 'test_sample' is name of the schema
SET (name, url) = ('Yahoo', 'http://yahoo.com')
WHERE link.name = 'Bing';
```

Short-hand notation to update values from model data:

```
yahoo := model.Link{
    URL:  "http://www.yahoo.com",
    Name: "Yahoo",
}

updateStmt := Link.
    UPDATE(Link.Name, Link.URL, Link.Description).
    MODEL(yahoo).
    WHERE(Link.Name.EQ(String("Bing")))
```

`Link.Name, Link.URL, Link.Description` - can be replaced with Link.MutableColumns(all columns minus primary key column).  
Primary key columns usually are not updated.

```
updateStmt := Link.
    UPDATE(Link.MutableColumns).
    MODEL(yahoo).
    WHERE(Link.Name.EQ(String("Bing")))
```

### Execute statement

To execute update statement and get sql.Result:

```
res, err := updateStmt.Exec(db)
```

To execute update statement and return row records updated, statement has to have RETURNING clause:
```
updateStmt := Link.
    UPDATE(Link.MutableColumns).
    MODEL(yahoo).
    WHERE(Link.Name.EQ(String("Bing"))).
    RETURNING(Link.AllColumns)
    
dest := []model.Link{}

err := updateStmt.Query(db, &dest)   
```

Use `ExecContext` and `QueryContext` to provide context object to execution.

##### SQL table used for the example:
```sql
CREATE TABLE IF NOT EXISTS link (
    id serial PRIMARY KEY,
    url VARCHAR (255) NOT NULL,
    name VARCHAR (255) NOT NULL,
    description VARCHAR (255)
);
```

