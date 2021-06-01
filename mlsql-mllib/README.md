# mlsql-mmlib

This plugin provide ET wrapper for spark-mllib.

## Install from store

Execute following command in web console:

```
!plugin app add "tech.mlsql.plugins.mllib.app.MLSQLMllib" "mlsql-mllib-2.4";
```

Check installation:

```
!show et/ClassificationEvaluator;
!show etc/RegressionEvaluator;
```

## Install Manually

Firstly, build shade jar in your terminal:

```shell
pip install mlsql_plugin_tool
mlsql_plugin_tool build --module_name mlsql-mllib --spark spark243
```

then change start script of MLSQL Engine,

Add Jar:

```
--jars YOUR_JAR_PATH
```

Register Class:

```
-streaming.plugin.clzznames tech.mlsql.plugins.mllib.app.MLSQLMllib
```

If there are more than one class, use comma to seperate them. For example:

```
-streaming.plugin.clzznames classA,classB,classC
```

## Usage

Classification:

```sql
predict data as RandomForest.`/tmp/model` as predicted_table;
run predicted_table as ClassificationEvaluator.``;
```

Regression:

```sql
predict data as LinearRegressionExt.`/tmp/model` as predicted_table;
run predicted_table as RegressionEvaluator.``;
```







