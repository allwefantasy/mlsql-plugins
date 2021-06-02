
for spark_version in spark243 spark311
do
./install.sh mlsql-shell ${spark_version}
./install.sh mlsql-mllib ${spark_version}
./install.sh connect-persist ${spark_version}
./install.sh last-command ${spark_version}
./install.sh mlsql-excel ${spark_version}
./install.sh run-script ${spark_version}
./install.sh save-then-load ${spark_version}
./install.sh stream-persist ${spark_version}
./install.sh table-repartition ${spark_version}
done


# ./install.sh ds-hbase-2x
# ./install.sh mlsql-bigdl