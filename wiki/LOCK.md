
LOCK statement obtains a table-level lock, waiting if necessary for any conflicting locks to be released.
More about LOCK statement in PostgreSQL: https://www.postgresql.org/docs/11/sql-lock.html  

Following clauses are supported:
- IN(mode) - mode specifies which locks this lock conflicts with.  
Mode can be: 
    - jet.LOCK_ACCESS_SHARE          
    - jet.LOCK_ROW_SHARE             
    - jet.LOCK_ROW_EXCLUSIVE         
    - jet.LOCK_SHARE_UPDATE_EXCLUSIVE
    - jet.LOCK_SHARE                 
    - jet.LOCK_SHARE_ROW_EXCLUSIVE   
    - jet.LOCK_EXCLUSIVE             
    - jet.LOCK_ACCESS_EXCLUSIVE      
- NOWAIT() - locked table should not wait for any conflicting locks to be released. If the specified lock(s) 
cannot be acquired immediately without waiting, the transaction is aborted.

## Example


```
lockStmt := Address.
        LOCK().
        IN(jet.LOCK_ACCESS_SHARE).
        NOWAIT()
```

Debug SQL of above statement:

```sql
LOCK TABLE dvds.address IN ACCESS SHARE MODE NOWAIT;
```

### Execute statement

To execute update statement and get sql.Result:

```
res, err := lockStmt.Exec(db)
```

Use `ExecContext` to provide context object to execution.

 