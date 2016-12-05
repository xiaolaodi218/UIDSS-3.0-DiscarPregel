#!/bin/bash

#
#arg(1) current month, for ex. "201609" .
#

baseDirForScriptSelf=$(cd "$(dirname "$0")"; pwd)

cur_month=$1
if [ "${cur_month}" = "" ] ; then
	cur_month=$(date -d last-month "+%Y%m")
fi

{
#
echo "snapshot 'dev_yx:UID_GRAPH', 'UID_GRAPH_SNAP_"${cur_month}"'"
#
echo "clone_snapshot 'UID_GRAPH_SNAP_"${cur_month}"' , 'dev_yx:UID_GRAPH_"${cur_month}"'"
echo "exit"
}>backup.hbaseshell

sleep 3

hbase shell backup.hbaseshell




