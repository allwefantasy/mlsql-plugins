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
mlsql_plugin_tool build --module_name xxxxx
```

Once build success, the system will show message like fowllowing:

```

====Build success!=====
 File location 0：
 /Users/allwefantasy/Volumes/Samsung_T5/allwefantasy/CSDNWorkSpace/mlsqlplugins/ds-hbase-2x/target/ds-hbase-2x-2.4_2.11-0.1.0-SNAPSHOT.jar

```

Then you can install this jar in [MLSQL Engine](https://docs.mlsql.tech/mlsql-stack/plugin/offline_install.html)


##
