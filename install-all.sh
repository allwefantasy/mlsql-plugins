#!/usr/bin/env bash

ALL_MODUELS="mlsql-shell mlsql-assert mlsql-mllib mlsql-excel connect-persist last-command run-script save-then-load stream-persist table-repartition"

MODUELS=${1}

if [[ "${MODUELS}" == "" ]];then
   MODUELS = ALL_MODUELS
fi

for spark_version in spark243 spark311
do
  for module in ${MODUELS}
  do
     ./install.sh ${module} ${spark_version}
  done
done


# ./install.sh ds-hbase-2x
# ./install.sh mlsql-bigdl