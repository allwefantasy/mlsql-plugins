export module=${1}
if [[ -z "${module}" ]];
then
echo "Please specify module name"
exit -1
fi

export plugin_type=${2}
if [[ -z "${plugin_type}" ]];
then
echo "Please specify module type"
exit -1
fi

export plugin_type_number="0"

if [[ ${plugin_type} == "et" ]];then
plugin_type_number = 0
elif [[ ${plugin_type} == "ds" ]]; then
plugin_type_number = 1
elif [[ ${plugin_type} == "script" ]]; then
plugin_type_number = 2
elif [[ ${plugin_type} == "app" ]]; then
plugin_type_number = 3
fi


scala_version="2.11"
plugin_version="0.1.0-SNAPSHOT"
plugin_jar="${module}_${scala_version}-${plugin_version}.jar"

repo_location_prefix="/data/mlsql/store/repo/repo/plugins/download/"

mvn -DskipTests clean install -pl ${module} && scp ${module}/target/${plugin_jar} mlsql-official:${repo_location_prefix}

## read config file


cat << EOF
INSERT INTO `plugins_store` (`name`, `location`, `official`, `enable`, `author`, `version`, `mlsql_versions`, `created_time`, `github_url`, `plugin_type`, `desc`, `main_class`)
VALUES
('run_script',
'${repo_location_prefix}/${plugin_jar}',
0,
0,
'allwefantasy',
'${plugin_version}',
'1.5.0,1.5.0-SNAPSHOT',
0,
'https://github.com/allwefantasy/mlslq',
${plugin_type_number},
'',
'tech.mlsql.plugin.et.DeltaCommand');
EOF


