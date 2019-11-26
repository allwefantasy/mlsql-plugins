## Install command:

```
!plugin et add tech.mlsql.plugins.et.LastCommand last_command named last_command;
```

## Summary

When you want to get the result from command and used in next command(SQL), 
you can use `!last_command` command to get the table in prvievious command generate.

## Help


```sql
!show et LastCommand;
```

## Usage

```sql
!hdfs -ls /tmp/;
!last_command named hdfsTmpTable;
select * from hdfsTmpTable as output;
```





