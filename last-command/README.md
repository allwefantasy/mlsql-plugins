## Install

```
!plugin et add tech.mlsql.plugins.et.LastCommand last-command named lastCommand;
```

## Help


```sql
!show et LastCommand;
```

## Usage

```sql
!hdfs -ls /tmp/;
!lastCommand named hdfsTmpTable;
select * from hdfsTmpTable as output;
```





