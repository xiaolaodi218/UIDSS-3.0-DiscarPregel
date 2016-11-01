#!/bin/bash

#
#arg(1) current month, for ex. "201609" .
#arg(2) previous month, for example "201608". Or "" ,if there is no old data.

baseDirForScriptSelf=$(cd "$(dirname "$0")"; pwd)
logPath=/data11/dacp/mt001/UIDSS/logs/
cd ${baseDirForScriptSelf}

cur_month=$1
pre_month=$2
if [ "${cur_month}" = "" ] ; then
	cur_month=$(date -d last-month "+%Y%m")
fi

if [ "${pre_month}" = "" ] ; then
	pre_month=$(date -d '-2 months' "+%Y%m")
fi

mbl_dir=/daas/subtl/st001/uid/result1/uid_info_mbl_
tel_dir=/daas/subtl/st001/uid/result1/uid_info_tel_
wb_dir=/daas/subtl/st001/uid/result1/uid_info_wb_
array_province=(85133)

for i in "${!array_province[@]}"
do
  if [ "${pre_month}" = "0" ] ; then
    #Full dataset load
    #Internal mobile user data
    spark-submit --class cn.ctyun.UIDSS.UIDSS  --master yarn     --deploy-mode cluster  --driver-memory 10g  --num-executors 5    --executor-memory 10g  --executor-cores 1    --queue qyx1  UIDSS-0.30-jar-with-dependencies.jar Y_LoadRawData UID_INFO_MBL ${mbl_dir}${array_province[i]}/${cur_month}/ 0 &
    #Internal fix-line user data
    spark-submit --class cn.ctyun.UIDSS.UIDSS  --master yarn     --deploy-mode cluster    --driver-memory 10g  --num-executors 5    --executor-memory 10g      --executor-cores 1    --queue qyx1  UIDSS-0.30-jar-with-dependencies.jar  Y_LoadRawData UID_INFO_TEL    ${tel_dir}${array_province[i]}/${cur_month}/ 0 &
    #Internal wide-band user data
		spark-submit --class cn.ctyun.UIDSS.UIDSS  --master yarn     --deploy-mode cluster    --driver-memory 10g  --num-executors 5    --executor-memory 10g      --executor-cores 1    --queue qyx1  UIDSS-0.30-jar-with-dependencies.jar  Y_LoadRawData UID_INFO_WB    ${wb_dir}${array_province[i]}/${cur_month}/ 0 &
  else
    #Incremental data load
    #Internal mobile user data
    spark-submit --class cn.ctyun.UIDSS.UIDSS  --master yarn     --deploy-mode cluster  --driver-memory 10g  --num-executors 5    --executor-memory 10g  --executor-cores 1    --queue qyx1  UIDSS-0.30-jar-with-dependencies.jar Y_LoadRawData UID_INFO_MBL ${mbl_dir}${array_province[i]}/${cur_month}/ ${mbl_dir}${array_province[i]}/${pre_month}/ &
    #Internal fix-line user data
    spark-submit --class cn.ctyun.UIDSS.UIDSS  --master yarn     --deploy-mode cluster    --driver-memory 10g  --num-executors 5    --executor-memory 10g      --executor-cores 1    --queue qyx1  UIDSS-0.30-jar-with-dependencies.jar  Y_LoadRawData UID_INFO_TEL    ${tel_dir}${array_province[i]}/${cur_month}/ ${mbl_dir}${array_province[i]}/${pre_month}/ &
    #Internal wide-band user data
    spark-submit --class cn.ctyun.UIDSS.UIDSS  --master yarn     --deploy-mode cluster    --driver-memory 10g  --num-executors 5    --executor-memory 10g      --executor-cores 1    --queue qyx1  UIDSS-0.30-jar-with-dependencies.jar  Y_LoadRawData UID_INFO_WB    ${wb_dir}${array_province[i]}/${cur_month}/ ${mbl_dir}${array_province[i]}/${pre_month}/ &
  fi
  printf "Internal data ${array_province[i]}/${cur_month}/ is loaded\n"
done
