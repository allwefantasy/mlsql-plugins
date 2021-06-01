# mlsql-plugins

This project is a collection of plugins for MLSQL.
Please check every module in project for more detail.

## Build Shade Jar

Requirements:

1. Python >= 3.6
2. Maven >= 3.0

You can install [mlsql_plugin_tool](https://github.com/allwefantasy/mlsql_plugin_tool) to build module in this project.

Install command:

```
pip install mlsql_plugin_tool
```

Build shard jar comamnd:

```
mlsql_plugin_tool build --module_name xxxxx --spark spark243
```

1. spark: two options are avaiable, spark243, spark311
2. module_name e.g mlsql-excel, ds-hbase-2x

Once build success, the system will show message like fowllowing:

```

====Build success!=====
 File location 0：
 /Users/allwefantasy/Volumes/Samsung_T5/allwefantasy/CSDNWorkSpace/mlsqlplugins/ds-hbase-2x/target/ds-hbase-2x-2.4_2.11-0.1.0-SNAPSHOT.jar

```

Then you can install this plugin(jar file) in [MLSQL Engine](https://docs.mlsql.tech/mlsql-stack/plugin/offline_install.html)

## Plugins which Both Support Spark 2.4.3/3.1.1

1. binlog2delta
2. connect-persist
3. ds-hbase-2x
4. mlsql-bigdl
5. mlsql-excel
6. stream-persist
7. mlsql-mllib