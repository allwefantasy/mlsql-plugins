## Install command:

```
!plugin script add - binlog2delta;
```

## Usage

```sql
set checkpointLocation="/tmp/cpl-binlog-m";
include plugin.`binlog2delta`;
```

Here are parameter you can set before include the plugin:

```sql
set streamName="binlog";

set host="127.0.0.1";
set port="3306";
set userName="root";
set password="mlsql";
set bingLogNamePrefix="mysql-bin";
set binlogIndex="1";
set binlogFileOffset="4";
set databaseNamePattern="mlsql_console";
set tableNamePattern="script_file";

set deltaTableHome="/tmp/binlog2delta";
set idCols="id";
set duration="10";
set checkpointLocation="/tmp/ck-binlog2delta";

``` 

## Check the content in plugin

```sql
!plugin script show binlog2delta/plugin.json;
```

or

```sql
!plugin script show binlog2delta/main.json;
``` 

