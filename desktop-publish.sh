SOURCE=/Users/allwefantasy/Volumes/Samsung_T5/allwefantasy/CSDNWorkSpace/mlsqlplugins
TARGET=/Users/allwefantasy/projects/mlsql-desktop
#conda activate mlsql-plugin-tool

mods=${1:-mlsql-language-server mlsql-excel mlsql-assert mlsql-shell}

for mod in ${mods}
do
  echo "build= $mod"
  mlsql_plugin_tool build --module_name ${mod} --spark spark311

  for os in linux mac win
  do
     cp ${SOURCE}/$mod/build/${mod}-3.0_2.12-0.1.0-SNAPSHOT.jar ${TARGET}/${os}/plugin
  done
done

#mlsql-language-server mlsql-excel mlsql-assert mlsql-shell
