#!/bin/bash

baseDirForScriptSelf=$(cd "$(dirname "$0")"; pwd)
logPath=/data11/dacp/mt001/uidss/logs/

if [ $# = 0 ] ; then
DAY=`date -d "1 days ago" +"%Y%m%d"`

#�����ⲿ�ƶ��û�����
nohup spark-submit --class cn.ctyun.UIDSS.UIDSS  --master yarn     --deploy-mode cluster    --driver-memory 10g  --num-executors 5    --executor-memory 10g     --executor-cores 1    --queue qyx1  UIDSS-0.10-jar-with-dependencies.jar  Y_LoadRawData DPI_INFO_MBL     /daas/subtl/st001/label_asiainfo/sichuan/present_quick_mobile/20160525/    0   &

#�����ⲿ�����û�����
nohup spark-submit --class cn.ctyun.UIDSS.UIDSS  --master yarn     --deploy-mode cluster    --driver-memory 10g  --num-executors 1     --executor-memory 10g     --executor-cores 1    --queue qyx1  UIDSS-0.10-jar-with-dependencies.jar  Y_LoadRawData DPI_INFO_WB     /daas/subtl/st001/label_asiainfo/sichuan/present_quick_fix_accntMD5/20160525/000000_0_clr    0   &