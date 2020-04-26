## Install command:

```
!plugin app add - "stream-persist-app-2.4";
```

The first plugin is APP plugin and the second is ET plugin.


## Usage

Use ET Plugin to persist stream job.

```sql
!streamPersist persist streamExample;

!streamPersist remove streamExample;

!streamPersist list;
```

And then once MLSQL Engine is restarted, and the stream job streamExample will be 
boosted at the startup of MLSQL.



