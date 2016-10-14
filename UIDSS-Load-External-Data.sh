#!/bin/bash

#
#arg(1) current date, for ex. "20160525" .

baseDirForScriptSelf=$(cd "$(dirname "$0")"; pwd)
logPath=/data11/dacp/mt001/UIDSS/logs/

cur_month=$1

mbl_dir=present_quick_mobile
wb_dir=present_quick_fix_accntMD5
array_province=(sichuan)

for i in "${!array_province[@]}"
do
	#加载外部移动用户数据
	spark-submit --class cn.ctyun.UIDSS.UIDSS  --master yarn     --deploy-mode cluster    --driver-memory 10g  --num-executors 5    --executor-memory 10g     --executor-cores 1    --queue qyx1  UIDSS-0.30-jar-with-dependencies.jar  Y_LoadRawData DPI_INFO_MBL     /daas/subtl/st001/label_asiainfo/${array_province[i]}/${mbl_dir}/${cur_month}/    0   &
	#加载外部固网用户数据
	spark-submit --class cn.ctyun.UIDSS.UIDSS  --master yarn     --deploy-mode cluster    --driver-memory 10g  --num-executors 5     --executor-memory 10g     --executor-cores 1    --queue qyx1  UIDSS-0.30-jar-with-dependencies.jar  Y_LoadRawData DPI_INFO_WB     /daas/subtl/st001/label_asiainfo/${array_province[i]}/${wb_dir}/${cur_month}/000000_0_clr    0   &

  printf "External data ${array_province[i]}/${cur_month} is loaded\n"
        
done
