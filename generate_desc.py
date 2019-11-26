
sql='''
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
'https://github.com/allwefantasy/deltaehancer',
${plugin_type_number},
'',
'tech.mlsql.plugin.et.DeltaCommand');
'''


