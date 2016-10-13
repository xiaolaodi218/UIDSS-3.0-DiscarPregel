#!/bin/bash

baseDirForScriptSelf=$(cd "$(dirname "$0")"; pwd)
logPath=/data11/dacp/mt001/uidss/logs/

if [ $# = 0 ] ; then
DAY=`date -d "1 days ago" +"%Y%m%d"`

#创建当月的快照
snapshot 'dev_yx:UID_GRAPH', 'UID_GRAPH_SNAP_201603'
#创建当月的关系表克隆表
clone_snapshot'UID_GRAPH_SNAP_201603' , ' dev_yx:UID_GRAPH_201603'
