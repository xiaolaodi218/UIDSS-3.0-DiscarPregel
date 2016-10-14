#!/bin/bash

#
#arg(1) current month, for ex. "201609" .
#

baseDirForScriptSelf=$(cd "$(dirname "$0")"; pwd)
logPath=/data11/dacp/mt001/UIDSS/logs/

cur_month=$1

#�������µĿ���
snapshot 'dev_yx:UID_GRAPH', 'UID_GRAPH_SNAP_"${cur_month}"'
#�������µĹ�ϵ���¡��
clone_snapshot'UID_GRAPH_SNAP_"${cur_month}"' , ' dev_yx:UID_GRAPH_"${cur_month}"'
