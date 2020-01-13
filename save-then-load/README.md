## Install 

```sql
!plugin et add tech.mlsql.plugins.et.SaveThenLoad "save-then-load" named saveThenLoad;
```

## Usage

This plugin will save the table into delta table and load it again.

```sql
!saveThenLoad tableName;
select * from tableName as output;
```