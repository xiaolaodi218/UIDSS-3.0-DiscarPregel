#!/bin/bash

function __readINI() {
 INIFILE=$1; SECTION=$2; ITEM=$3
 _readIni=`awk -F '=' '/\['$SECTION'\]/{a=1}a==1&&$1~/'$ITEM'/{print $2;exit}' $INIFILE`
echo ${_readIni}
}

#
#arg(1) current month, for ex. "201609" .
#arg(2) previous month, for example "201608". Or "" ,if there is no old data.

baseDirForScriptSelf=$(cd "$(dirname "$0")"; pwd)
cd ${baseDirForScriptSelf}

cur_month=$1
pre_month=$2
if [ "${cur_month}" = "" ] ; then
	cur_month=$(date -d last-month "+%Y%m")
fi

if [ "${pre_month}" = "" ] ; then
	pre_month=$(date -d '-2 months' "+%Y%m")
fi

hdfs_base_dir=$(__readINI UIDSS-Shell.ini LoadInternal hdfs_base_dir)
mbl_dir=$(__readINI UIDSS-Shell.ini LoadInternal mbl_dir)
wb_dir=$(__readINI UIDSS-Shell.ini LoadInternal wb_dir)
tel_dir=$(__readINI UIDSS-Shell.ini LoadInternal tel_dir)
array_province=($(__readINI UIDSS-Shell.ini LoadInternal province))
yarn_queue=$(__readINI UIDSS-Shell.ini LoadInternal yarn_queue)

for i in "${!array_province[@]}"
do
  if [ "${pre_month}" = "0" ] ; then
    #Full dataset load
    #Internal mobile user data
    spark-submit --class cn.ctyun.UIDSS.UIDSS  --master yarn     --deploy-mode cluster  --driver-memory 10g  --num-executors 5    --executor-memory 10g  --executor-cores 1    --queue ${yarn_queue}  UIDSS-0.30-jar-with-dependencies.jar Y_LoadRawData UID_INFO_MBL ${hdfs_base_dir}/${mbl_dir}/${array_province[i]}/${cur_month}/ 0 &
    #Internal fix-line user data
    spark-submit --class cn.ctyun.UIDSS.UIDSS  --master yarn     --deploy-mode cluster    --driver-memory 10g  --num-executors 5    --executor-memory 10g      --executor-cores 1    --queue ${yarn_queue}  UIDSS-0.30-jar-with-dependencies.jar  Y_LoadRawData UID_INFO_TEL   ${hdfs_base_dir}/${tel_dir}/${array_province[i]}/${cur_month}/ 0 &
    #Internal wide-band user data
		spark-submit --class cn.ctyun.UIDSS.UIDSS  --master yarn     --deploy-mode cluster    --driver-memory 10g  --num-executors 5    --executor-memory 10g      --executor-cores 1    --queue ${yarn_queue}  UIDSS-0.30-jar-with-dependencies.jar  Y_LoadRawData UID_INFO_WB   ${hdfs_base_dir}/${wb_dir}/${array_province[i]}/${cur_month}/ 0 &
  else
    #Incremental data load
    #Internal mobile user data
    spark-submit --class cn.ctyun.UIDSS.UIDSS  --master yarn     --deploy-mode cluster  --driver-memory 10g  --num-executors 5    --executor-memory 10g  --executor-cores 1    --queue ${yarn_queue}  UIDSS-0.30-jar-with-dependencies.jar Y_LoadRawData UID_INFO_MBL  ${hdfs_base_dir}/${mbl_dir}/${array_province[i]}/${cur_month}/ ${hdfs_base_dir}/${mbl_dir}/${array_province[i]}/${pre_month}/ &
    #Internal fix-line user data
    spark-submit --class cn.ctyun.UIDSS.UIDSS  --master yarn     --deploy-mode cluster    --driver-memory 10g  --num-executors 5    --executor-memory 10g      --executor-cores 1    --queue ${yarn_queue}  UIDSS-0.30-jar-with-dependencies.jar  Y_LoadRawData UID_INFO_TEL    ${hdfs_base_dir}/${tel_dir}/${array_province[i]}/${cur_month}/ ${hdfs_base_dir}/${tel_dir}/${array_province[i]}/${pre_month}/ &
    #Internal wide-band user data
    spark-submit --class cn.ctyun.UIDSS.UIDSS  --master yarn     --deploy-mode cluster    --driver-memory 10g  --num-executors 5    --executor-memory 10g      --executor-cores 1    --queue ${yarn_queue}  UIDSS-0.30-jar-with-dependencies.jar  Y_LoadRawData UID_INFO_WB    ${hdfs_base_dir}/${wb_dir}/${array_province[i]}/${cur_month}/ ${hdfs_base_dir}/${wb_dir}/${array_province[i]}/${pre_month}/ &
  fi
  printf "Internal data ${array_province[i]}/${cur_month}/ is loaded\n"
done
