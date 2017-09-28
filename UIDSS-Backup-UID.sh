#!/bin/bash

#
#arg(1) current month, for ex. "201609" .
#
function __readINI() {
 INIFILE=$1; SECTION=$2; ITEM=$3
 _readIni=`awk -F '=' '/\['$SECTION'\]/{a=1}a==1&&$1~/'$ITEM'/{print $2;exit}' $INIFILE`
echo ${_readIni}
}

baseDirForScriptSelf=$(cd "$(dirname "$0")"; pwd)
cd ${baseDirForScriptSelf}

hbase_user_space=$(__readINI UIDSS-Shell.ini BackUp hbase_user_space)

cur_month=$1
if [ "${cur_month}" = "" ] ; then
	cur_month=$(date -d last-month "+%Y%m")
fi

{
#
echo "snapshot '"${hbase_user_space}":UID_GRAPH', 'UID_GRAPH_SNAP_"${cur_month}"'"
#
echo "clone_snapshot 'UID_GRAPH_SNAP_"${cur_month}"' , '"${hbase_user_space}":UID_GRAPH_"${cur_month}"'"
echo "exit"
}>backup.hbaseshell

sleep 3

hbase shell backup.hbaseshell




