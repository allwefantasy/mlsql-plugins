mlsql-canal

Used in streaming, parse canal binlog, store it to delta lake, support ddl.
Only support spark 3.X.

## Install

```
!plugin ds add - "mlsql-canal-3.0";
```

or install as app:

```
!plugin app add "tech.mlsql.plugins.canal.CanalApp" "mlsql-canal-3.0";
```


## Usage

```sql
set streamName="binlog_to_delta";

load kafka.`binlog-canal_test`
options `kafka.bootstrap.servers` = "***"
 and `maxOffsetsPerTrigger`="600000"
as kafka_record;

select cast(value as string) as value from kafka_record
as kafka_value;

save append kafka_value
as custom.``
options mode = "Append"
and duration = "20"
and sourceTable = "kafka_value"
and checkpointLocation = "checkpoint/binlog_to_delta"
and code = '''
run kafka_value
as BinlogToDelta.``
options dbTable = "canal_test.test";
''';
```