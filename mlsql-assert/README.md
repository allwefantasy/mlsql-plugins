# mlsql-shell

This plugin provide assert in table.


## Install from store

Execute following command in web console:

```
!plugin app add - "mlsql-assert-2.4";
```


## Install Manually

Firstly, build shade jar in your terminal:

```shell
pip install mlsql_plugin_tool
mlsql_plugin_tool build --module_name mlsql-assert --spark spark243
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

-- !plugin app remove "mlsql-assert-2.4";
-- !plugin app add - "mlsql-assert-2.4";
-- create test data
set jsonStr='''
{"features":[5.1,3.5,1.4,0.2],"label":0.0},
{"features":[5.1,3.5,1.4,0.2],"label":1.0}
{"features":[5.1,3.5,1.4,0.2],"label":0.0}
{"features":[4.4,2.9,1.4,0.2],"label":0.0}
{"features":[5.1,3.5,1.4,0.2],"label":1.0}
{"features":[5.1,3.5,1.4,0.2],"label":0.0}
{"features":[5.1,3.5,1.4,0.2],"label":0.0}
{"features":[4.7,3.2,1.3,0.2],"label":1.0}
{"features":[5.1,3.5,1.4,0.2],"label":0.0}
{"features":[5.1,3.5,1.4,0.2],"label":0.0}
''';
load jsonStr.`jsonStr` as data;
select vec_dense(features) as features ,label as label from data
as data1;

-- use RandomForest
train data1 as RandomForest.`/tmp/model` where

-- once set true,every time you run this script, MLSQL will generate new directory for you model
keepVersion="true"

-- specicy the test dataset which will be used to feed evaluator to generate some metrics e.g. F1, Accurate
and evaluateTable="data1"

-- specify group 0 parameters
and `fitParam.0.labelCol`="features"
and `fitParam.0.featuresCol`="label"
and `fitParam.0.maxDepth`="2"

-- specify group 1 parameters
and `fitParam.1.featuresCol`="features"
and `fitParam.1.labelCol`="label"
and `fitParam.1.maxDepth`="10"
as model_result;

select name,value from model_result where name="status" as result;
-- make sure status of  all models are success.  
!assert result ''':value=="success"'''  "all model status should be success";

```









