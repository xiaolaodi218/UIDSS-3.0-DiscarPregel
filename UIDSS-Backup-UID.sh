#!/bin/bash

baseDirForScriptSelf=$(cd "$(dirname "$0")"; pwd)
logPath=/data11/dacp/mt001/uidss/logs/

if [ $# = 0 ] ; then
DAY=`date -d "1 days ago" +"%Y%m%d"`

#�������µĿ���
snapshot 'dev_yx:UID_GRAPH', 'UID_GRAPH_SNAP_201603'
#�������µĹ�ϵ���¡��
clone_snapshot'UID_GRAPH_SNAP_201603' , ' dev_yx:UID_GRAPH_201603'
