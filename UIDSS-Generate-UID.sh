#!/bin/bash

baseDirForScriptSelf=$(cd "$(dirname "$0")"; pwd)
logPath=/data11/dacp/mt001/UIDSS/logs/

spark-submit --class cn.ctyun.UIDSS.UIDSS  --master yarn     --deploy-mode cluster    --driver-memory 10g  --executor-memory 30g  --num-executors 5  --executor-cores 1  --queue qyx1  UIDSS-0.30-jar-with-dependencies.jar  Y_GenerateUIDExt &
