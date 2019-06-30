DELETE statement deletes rows that satisfy the WHERE clause from the specified table. More about delete statement
in PostgreSQL: https://www.postgresql.org/docs/11/sql-delete.html

Following clauses are supported:
- WHERE(delete_condition) - Only rows for which delete condition returns true will be deleted.
- RETURNING(output_expression...) - An expressions to be computed and returned by the DELETE command after each row is deleted. 
The expression can use any column names of the table. Write _TableName_.AllColumns to return all columns.

### Example

```
// delete all links with name 'Gmail' and 'Outlook'
deleteStmt := Link.
    DELETE().
    WHERE(Link.Name.IN(String("Gmail"), String("Outlook")))
```

Debug sql of above statement:

```sql
DELETE FROM test_sample.link      -- test_sample is name of the schema
WHERE link.name IN ('Gmail', 'Outlook');
```

### Execute statement

To execute delete statement and get sql.Result:

```
res, err := deleteStmt.Exec(db)
```

To execute delete statement and return records deleted, 
delete statement has to have RETURNING clause:

```
deleteStmt := Link.
    DELETE().
    WHERE(Link.Name.IN(String("Gmail"), String("Outlook"))).
    RETURNING(Link.AllColumns)
    
dest := []model.Link{}

err := deleteStmt.Query(db, &dest)
    
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