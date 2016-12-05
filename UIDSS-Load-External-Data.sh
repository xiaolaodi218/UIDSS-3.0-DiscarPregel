#!/bin/bash

function __readINI() {
 INIFILE=$1; SECTION=$2; ITEM=$3
 _readIni=`awk -F '=' '/\['$SECTION'\]/{a=1}a==1&&$1~/'$ITEM'/{print $2;exit}' $INIFILE`
echo ${_readIni}
}

baseDirForScriptSelf=$(cd "$(dirname "$0")"; pwd)
cd ${baseDirForScriptSelf}

cur_month=$1
if [ "${cur_month}" = "" ] ; then
  cur_month=$(date -d last-month "+%Y%m")
fi

hdfs_base_dir=$(__readINI UIDSS-Shell.ini LoadExternal hdfs_base_dir)
mbl_dir=$(__readINI UIDSS-Shell.ini LoadExternal mbl_dir)
wb_dir=$(__readINI UIDSS-Shell.ini LoadExternal wb_dir)
array_province=($(__readINI UIDSS-Shell.ini LoadExternal province))
yarn_queue=$(__readINI UIDSS-Shell.ini LoadExternal yarn_queue)

for i in "${!array_province[@]}"
do
  #Load external mobile user data
  spark-submit --class cn.ctyun.UIDSS.UIDSS  --master yarn     --deploy-mode cluster    --driver-memory 10g  --num-executors 5    --executor-memory 10g     --executor-cores 1    --queue ${yarn_queue}  UIDSS-0.30-jar-with-dependencies.jar  Y_LoadRawData DPI_INFO_MBL     ${hdfs_base_dir}/${array_province[i]}/${mbl_dir}/${cur_month}/    0   &
  #Load external wide-band user data
  spark-submit --class cn.ctyun.UIDSS.UIDSS  --master yarn     --deploy-mode cluster    --driver-memory 10g  --num-executors 5     --executor-memory 10g     --executor-cores 1    --queue ${yarn_queue}  UIDSS-0.30-jar-with-dependencies.jar  Y_LoadRawData DPI_INFO_WB    ${hdfs_base_dir}/${array_province[i]}/${wb_dir}/${cur_month}/    0   &
  printf "External data ${array_province[i]}/${cur_month} is loaded\n"
done
