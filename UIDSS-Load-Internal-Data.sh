#!/bin/bash

#
#arg(1) current month, for ex. "201609" .
#arg(2) previous month, for example "201608". Or "" ,if there is no old data.

baseDirForScriptSelf=$(cd "$(dirname "$0")"; pwd)
logPath=/data11/dacp/mt001/UIDSS/logs/

cur_month=$1
pre_month=$2

mbl_dir=/daas/subtl/st001/uid/result1/uid_info_mbl_
tel_dir=/daas/subtl/st001/uid/result1/uid_info_tel_
wb_dir=/daas/subtl/st001/uid/result1/uid_info_wb_
array_province=(85133)

for i in "${!array_province[@]}"
do
        if [ "${pre_month}" = "" ] ; then
                #全量加载
                #内部移动用户数据
                spark-submit --class cn.ctyun.UIDSS.UIDSS  --master yarn     --deploy-mode cluster  --driver-memory 10g  --num-executors 5    --executor-memory 10g  --executor-cores 1    --queue qyx1  UIDSS-0.30-jar-with-dependencies.jar Y_LoadRawData UID_INFO_MBL ${mbl_dir}${array_province[i]}/${cur_month} 0 
                #内部固网用户数据
								spark-submit --class cn.ctyun.UIDSS.UIDSS  --master yarn     --deploy-mode cluster    --driver-memory 10g  --num-executors 5    --executor-memory 10g      --executor-cores 1    --queue qyx1  UIDSS-0.30-jar-with-dependencies.jar  Y_LoadRawData UID_INFO_TEL    ${tel_dir}${array_province[i]}/${cur_month} 0 
								#内部宽带用户数据
								spark-submit --class cn.ctyun.UIDSS.UIDSS  --master yarn     --deploy-mode cluster    --driver-memory 10g  --num-executors 5    --executor-memory 10g      --executor-cores 1    --queue qyx1  UIDSS-0.30-jar-with-dependencies.jar  Y_LoadRawData UID_INFO_WB    ${wb_dir}${array_province[i]}/${cur_month} 0 
                
        else
                #增量加载
                #内部移动用户数据
                spark-submit --class cn.ctyun.UIDSS.UIDSS  --master yarn     --deploy-mode cluster  --driver-memory 10g  --num-executors 5    --executor-memory 10g  --executor-cores 1    --queue qyx1  UIDSS-0.30-jar-with-dependencies.jar Y_LoadRawData UID_INFO_MBL ${mbl_dir}${array_province[i]}/${cur_month} ${mbl_dir}${array_province[i]}/${pre_month} 
                #内部固网用户数据
								spark-submit --class cn.ctyun.UIDSS.UIDSS  --master yarn     --deploy-mode cluster    --driver-memory 10g  --num-executors 5    --executor-memory 10g      --executor-cores 1    --queue qyx1  UIDSS-x.xx-jar-with-dependencies.jar  Y_LoadRawData UID_INFO_TEL    ${tel_dir}${array_province[i]}/${cur_month} ${mbl_dir}${array_province[i]}/${pre_month}
								#加载内部宽带用户数据
								spark-submit --class cn.ctyun.UIDSS.UIDSS  --master yarn     --deploy-mode cluster    --driver-memory 10g  --num-executors 5    --executor-memory 10g      --executor-cores 1    --queue qyx1  UIDSS-x.xx-jar-with-dependencies.jar  Y_LoadRawData UID_INFO_WB    ${wb_dir}${array_province[i]}/${cur_month} ${mbl_dir}${array_province[i]}/${pre_month}
        fi

        printf "Internal data ${array_province[i]}/${cur_month} is loaded\n"
        
done