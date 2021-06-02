# mlsql-shell

This plugin provide execute shell command in MLSQL Engine Driver Side.

![](http://store.mlsql.tech/upload_images/6d09a596-cb0a-495c-9a95-6bbcc63be9ab.png)

## Install from store

Execute following command in web console:

```
!plugin app add - "mlsql-shell-2.4";
```

Check installation:

```
!sh pip install pyjava;
```


## Install Manually

Firstly, build shade jar in your terminal:

```shell
pip install mlsql_plugin_tool
mlsql_plugin_tool build --module_name mlsql-shell --spark spark243
```

then change start script of MLSQL Engine,

Add Jar:

```
--jars YOUR_JAR_PATH
```

Register Class:

```
-streaming.plugin.clzznames tech.mlsql.plugins.shell.app.MLSQLShell
```

If there are more than one class, use comma to seperate them. For example:

```
-streaming.plugin.clzznames classA,classB,classC
```

## Usage

```sql
!sh pip install pyjava;
!sh echo "yes";
!sh wget "https://github.com/allwefantasy/mlsql-plugins/tree/master/mlsql-shell";
```









