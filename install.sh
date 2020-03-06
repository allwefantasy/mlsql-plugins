export module=${1}
if [[ -z "${module}" ]];
then
echo "Please specify module name"
exit -1
fi

sfcli release --deploy store --mlsql_store --module ${module} --mvn mvn --user ${STORE_USER} --password ${STORE_PASSWORD}



