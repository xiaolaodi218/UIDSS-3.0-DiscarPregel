#!/bin/bash

function __readINI() {
 INIFILE=$1; SECTION=$2; ITEM=$3
 _readIni=`awk -F '=' '/\['$SECTION'\]/{a=1}a==1&&$1~/'$ITEM'/{print $2;exit}' $INIFILE`
echo ${_readIni}
}

baseDirForScriptSelf=$(cd "$(dirname "$0")"; pwd)
cd ${baseDirForScriptSelf}

yarn_queue=$(__readINI UIDSS-Shell.ini GenerateUID yarn_queue)
driver_memory=$(__readINI UIDSS-Shell.ini GenerateUID driver_memory)
num_executors=$(__readINI UIDSS-Shell.ini GenerateUID num_executors)
executor_memory=$(__readINI UIDSS-Shell.ini GenerateUID executor_memory)
executor_cores=$(__readINI UIDSS-Shell.ini GenerateUID executor_cores)

spark-submit --class cn.ctyun.UIDSS.UIDSS  --master yarn  --deploy-mode cluster --driver-memory  ${driver_memory}  --executor-memory  ${executor_memory}  --num-executors  ${num_executors}  --executor-cores ${executor_cores}  --queue ${yarn_queue}  UIDSS-0.30-jar-with-dependencies.jar  Y_GenerateUIDExt &
