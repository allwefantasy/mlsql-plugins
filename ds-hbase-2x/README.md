## Install

```
!plugin ds add - ds-hbase-2x-2.4;
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
  .format("org.apache.spark.sql.execution.datasources.hbase2x")
  .save()
  
val df = spark.read.format("org.apache.spark.sql.execution.datasources.hbase2x").options(
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
set rawText='''
{"id":9,"content":"Spark好的语言1","label":0.0}
{"id":10,"content":"MLSQL是一个好的语言7","label":0.0}
{"id":12,"content":"MLSQL是一个好的语言7","label":0.0}
''';

load jsonStr.`rawText` as orginal_text_corpus;

select cast(id as String)  as rowkey,content,label from orginal_text_corpus as orginal_text_corpus1;

connect hbase2x where `zk`="127.0.0.1:2181"
and `family`="cf" as hbase1;

save overwrite orginal_text_corpus1 
as hbase2x.`hbase1:mlsql_example`;

load hbase2x.`hbase1:mlsql_example` where field.type.label="DoubleType"
as mlsql_example ;

select * from mlsql_example as show_data;
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




