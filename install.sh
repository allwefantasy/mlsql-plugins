export module=${1}
if [[ -z "${module}" ]];
then
echo "Please specify module name"
exit -1
fi
mvn -DskipTests clean install -pl ${module} && scp ${module}/target/${module}-0.1.0-SNAPSHOT.jar mlsql-official:/data/mlsql/store/repo/repo/plugins/download/
