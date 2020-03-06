## Install command:

```
!plugin app add - stream-boostrap-at-startup;
!plugin et add - et-stream-boostrap-at-startup named streambootstrapatstartup;

```

The first plugin is APP plugin and the second is ET plugin.


## Usage

Use ET Plugin to persist stream job.

```sql
!streambootstrapatstartup persist streamExample;
```

And then once MLSQL Engine is restarted, and the stream job streamExample will be 
boosted at the startup of MLSQL.



