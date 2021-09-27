PROJECT=/Users/allwefantasy/Volumes/Samsung_T5/allwefantasy/CSDNWorkSpace/mlsqlplugins

MOUDLE_NAME=$1
VERSION="0.1.0-SNAPSHOT"
V=${2:-3.0}
MIDDLE="2.4_2.11"

SPARK="spark311"

if [[ "${V}" == "2.4" ]]
then
   SPARK=spark243
fi

if [[ "${SPARK}" == "spark311" ]]
then
   MIDDLE="3.0_2.12"
fi

echo ${MOUDLE_NAME}
echo ${SPARK}
echo ${MIDDLE}

mlsql_plugin_tool build --module_name ${MOUDLE_NAME} --spark ${SPARK}
mlsql_plugin_tool upload \
--module_name ${MOUDLE_NAME}  \
--user ${STORE_USER}        \
--password ${STORE_PASSWORD} \
--jar_path ${PROJECT}/${MOUDLE_NAME}/build/${MOUDLE_NAME}-${MIDDLE}-${VERSION}.jar
