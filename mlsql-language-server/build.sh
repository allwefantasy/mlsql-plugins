SOURCE=/Users/allwefantasy/Volumes/Samsung_T5/allwefantasy/CSDNWorkSpace/mlsqlplugins/mlsql-language-server/build/
TARGET=/Users/allwefantasy/projects/mlsql/src/mlsql-lang/mlsql-app_2.4-2.1.0-SNAPSHOT/plugin
#conda activate mlsql-plugin-tool
mlsql_plugin_tool build --module_name mlsql-language-server --spark spark243
scp ${SOURCE}/mlsql-language-server-2.4_2.11-0.1.0-SNAPSHOT.jar ${TARGET}/
