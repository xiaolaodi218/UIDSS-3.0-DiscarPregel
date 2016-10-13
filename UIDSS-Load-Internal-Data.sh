#!/bin/bash

baseDirForScriptSelf=$(cd "$(dirname "$0")"; pwd)
logPath=/data11/dacp/mt001/uidss/logs/

if [ $# = 0 ] ; then
DAY=`date -d "1 days ago" +"%Y%m%d"`

#�����ڲ��ƶ��û�����
nohup spark-submit --class cn.ctyun.UIDSS.UIDSS  --master yarn     --deploy-mode cluster    --driver-memory 10g  --num-executors 1    --executor-memory 10g      --executor-cores 1    --queue qyx1  UIDSS-x.xx-jar-with-dependencies.jar  Y_LoadRawData UID_INFO_MBL    /daas/subtl/st001/uid/result1/uid_info_mbl_85133/201607     /daas/subtl/st001/uid/result1/uid_info_mbl_85133/201608   &
#�����ڲ������û�����
nohup spark-submit --class cn.ctyun.UIDSS.UIDSS  --master yarn     --deploy-mode cluster    --driver-memory 10g  --num-executors 1    --executor-memory 10g      --executor-cores 1    --queue qyx1  UIDSS-x.xx-jar-with-dependencies.jar  Y_LoadRawData UID_INFO_TEL    /daas/subtl/st001/uid/result1/uid_info_tel_85133/201607    /daas/subtl/st001/uid/result1/uid_info_tel_85133/201608    &
#�����ڲ�����û�����
nohup spark-submit --class cn.ctyun.UIDSS.UIDSS  --master yarn     --deploy-mode cluster    --driver-memory 10g  --num-executors 1    --executor-memory 10g      --executor-cores 1    --queue qyx1  UIDSS-x.xx-jar-with-dependencies.jar  Y_LoadRawData UID_INFO_WB    /daas/subtl/st001/uid/result1/uid_info_wb_85133/201607     /daas/subtl/st001/uid/result1/uid_info_wb_85133/201608     &
