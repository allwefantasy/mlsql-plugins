## Install command:

```
!plugin app add - 'connect-persist-app';
!plugin et add - 'connect-persist' named connectPersist;
```

The first plugin is APP plugin and the second is ET plugin.

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



