## Build

```
pip install mlsql_plugin_tool
mlsql_plugin_tool build --module_name mlsql-mllib --spark spark243
```

## Install

```
!plugin app add "tech.mlsql.plugins.mllib.app.MLSQLMllib "mlsql-mllib-2.4";
```

## Install at startup

spark-submit command line:

Add Jar:

```
--jars JAR_PATH
```

Add Class:

```
-streaming.plugin.clzznames tech.mlsql.plugins.mllib.app.MLSQLMllib
```

## Usage

```sql
predict data as RandomForest.`/tmp/model` as predicted_table;
run predicted_table as ClassificationEvaluator.``;
```





