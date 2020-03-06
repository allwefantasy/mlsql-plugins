## Install 

```sql
!plugin et add - "save-then-load" named saveThenLoad;
```

## Usage

This plugin will save the table into delta table and load it again.

```sql
!saveThenLoad tableName;
select * from tableName as output;
```