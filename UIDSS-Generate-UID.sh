#!/bin/bash

function __readINI() {
 INIFILE=$1; SECTION=$2; ITEM=$3
 _readIni=`awk -F '=' '/\['$SECTION'\]/{a=1}a==1&&$1~/'$ITEM'/{print $2;exit}' $INIFILE`
echo ${_readIni}
}

baseDirForScriptSelf=$(cd "$(dirname "$0")"; pwd)
cd ${baseDirForScriptSelf}

yarn_queue=$(__readINI UIDSS-Shell.ini GenerateUID yarn_queue)

spark-submit --class cn.ctyun.UIDSS.UIDSS  --master yarn  --deploy-mode cluster --driver-memory 10g  --executor-memory 30g  --num-executors 5  --executor-cores 1  --queue ${yarn_queue}  UIDSS-0.30-jar-with-dependencies.jar  Y_GenerateUIDExt &
