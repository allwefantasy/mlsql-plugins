## Install

```
!plugin ds add tech.mlsql.plugins.ds.MLSQLHBase2x ds-hbase-2x;
```

## Usage

DataFrame:

```scala
val data = (0 to 255).map { i =>
      HBaseRecord(i, "extra")
    }
val tableName = "t1"
val familyName = "c1"


import spark.implicits._
sc.parallelize(data).toDF.write
  .options(Map(
    "outputTableName" -> cat,
    "family" -> family
  ) ++ options)
  .format("org.apache.spark.sql.execution.datasources.hbase")
  .save()
  
val df = spark.read.format("org.apache.spark.sql.execution.datasources.hbase").options(
  Map(
    "inputTableName" -> tableName,
    "family" -> familyName,
    "field.type.col1" -> "BooleanType",
    "field.type.col2" -> "DoubleType",
    "field.type.col3" -> "FloatType",
    "field.type.col4" -> "IntegerType",
    "field.type.col5" -> "LongType",
    "field.type.col6" -> "ShortType",
    "field.type.col7" -> "StringType",
    "field.type.col8" -> "ByteType"
  )
).load() 
``` 

MLSQL: 

```sql
connect hbase where `zk`="127.0.0.1:2181"
and `family`="cf" as hbase1;

load hbase.`hbase1:mlsql_example`
as mlsql_example;

select * from mlsql_example as show_data;


select '2' as rowkey, 'insert test data' as name as insert_table;

save insert_table as hbase.`hbase1:mlsql_example`;
```

You should configure parameters like `zookeeper.znode.parent`,`hbase.rootdir` according by 
your HBase configuration.  

Parameters：

| Property Name  |  Meaning |
|---|---|
| tsSuffix |to overwrite hbase value's timestamp|
|namespace|hbase namespace|
| family |hbase family，family="" means load all existing families|
| field.type.ck | specify type for ck(field name),now supports:LongType、FloatType、DoubleType、IntegerType、BooleanType、BinaryType、TimestampType、DateType，default: StringType。|




