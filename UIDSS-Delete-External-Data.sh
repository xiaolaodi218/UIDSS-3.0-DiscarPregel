#!/bin/bash

function __readINI() {
 INIFILE=$1; SECTION=$2; ITEM=$3
 _readIni=`awk -F '=' '/\['$SECTION'\]/{a=1}a==1&&$1~/'$ITEM'/{print $2;exit}' $INIFILE`
echo ${_readIni}
}

#arg(1) obsolete date, for ex. "20161001000000" .

obsolete_date=$1
if [ "${obsolete_date}" = "" ] ; then
	obsolete_date=$(date -d '-3 months' "+%Y%m%d%H%M%S")
fi

baseDirForScriptSelf=$(cd "$(dirname "$0")"; pwd)
cd ${baseDirForScriptSelf}

yarn_queue=$(__readINI UIDSS-Shell.ini DeleteExternal yarn_queue)
driver_memory=$(__readINI UIDSS-Shell.ini DeleteExternal driver_memory)
num_executors=$(__readINI UIDSS-Shell.ini DeleteExternal num_executors)
executor_memory=$(__readINI UIDSS-Shell.ini DeleteExternal executor_memory)
executor_cores=$(__readINI UIDSS-Shell.ini DeleteExternal executor_cores)

spark-submit --class cn.ctyun.UIDSS.UIDSS  --master yarn     --deploy-mode cluster    --driver-memory ${driver_memory}  --executor-memory ${executor_memory}  --num-executors ${num_executors}  --executor-cores  ${executor_cores}  --queue ${yarn_queue}  UIDSS-0.30-jar-with-dependencies.jar Y_DeleteOldData ${obsolete_date}  
