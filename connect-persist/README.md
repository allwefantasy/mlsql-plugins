## Install command:

```
!plugin app add - 'connect-persist-app-2.4';
```

> Notice:
> If you set MLSQL meta store as MySQL, you should import db.sql file into  
> your meta database.

## Usage

Use ET Plugin to persist stream job.

```sql
!connectPersist;
```

And then once MLSQL Engine is restarted, and the connect info will be 
restored at the startup of MLSQL.



