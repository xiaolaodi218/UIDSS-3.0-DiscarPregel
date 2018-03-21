#!/bin/bash

function __readINI() {
 INIFILE=$1; SECTION=$2; ITEM=$3
 _readIni=`awk -F '=' '/\['$SECTION'\]/{a=1}a==1&&$1~/'$ITEM'/{print $2;exit}' $INIFILE`
echo ${_readIni}
}

baseDirForScriptSelf=$(cd "$(dirname "$0")"; pwd)
cd ${baseDirForScriptSelf}

cur_month=$1
if [ "${cur_month}" = "" ] ; then
  cur_month=$(date -d last-month "+%Y%m")
fi

hdfs_base_dir=$(__readINI UIDSS-Shell.ini LoadExternal hdfs_base_dir)
mbl_dir=$(__readINI UIDSS-Shell.ini LoadExternal mbl_dir)
wb_dir=$(__readINI UIDSS-Shell.ini LoadExternal wb_dir)
array_province=($(__readINI UIDSS-Shell.ini LoadExternal province))
yarn_queue=$(__readINI UIDSS-Shell.ini LoadExternal yarn_queue)
driver_memory_DPI_INFO_MBL=$(__readINI UIDSS-Shell.ini LoadExternal driver_memory_DPI_INFO_MBL)
num_executors_DPI_INFO_MBL=$(__readINI UIDSS-Shell.ini LoadExternal num_executors_DPI_INFO_MBL)
executor_memory_DPI_INFO_MBL=$(__readINI UIDSS-Shell.ini LoadExternal executor_memory_DPI_INFO_MBL)
executor_cores_DPI_INFO_MBL=$(__readINI UIDSS-Shell.ini LoadExternal executor_cores_DPI_INFO_MBL)
driver_memory_DPI_INFO_WB=$(__readINI UIDSS-Shell.ini LoadExternal driver_memory_DPI_INFO_WB)
num_executors_DPI_INFO_WB=$(__readINI UIDSS-Shell.ini LoadExternal num_executors_DPI_INFO_WB)
executor_memory_DPI_INFO_WB=$(__readINI UIDSS-Shell.ini LoadExternal executor_memory_DPI_INFO_WB)
executor_cores_DPI_INFO_WB=$(__readINI UIDSS-Shell.ini LoadExternal executor_cores_DPI_INFO_WB)

for i in "${!array_province[@]}"
do
  #Load external mobile user data
  spark-submit --class cn.ctyun.UIDSS.UIDSS  --master yarn     --deploy-mode cluster  --files /home/dal_pro/dal_pro.keytab  --driver-memory ${driver_memory_DPI_INFO_MBL}  --num-executors ${num_executors_DPI_INFO_MBL}    --executor-memory  ${executor_memory_DPI_INFO_MBL}   --executor-cores  ${executor_cores_DPI_INFO_MBL}    --queue ${yarn_queue}  UIDSS-3.0-jar-with-dependencies.jar  Y_LoadRawData DPI_INFO_MBL     ${hdfs_base_dir}/${mbl_dir}/prov_id=${array_province[i]}/mon_id=${cur_month}/    0
  wait 
  #Load external wide-band user data
  spark-submit --class cn.ctyun.UIDSS.UIDSS  --master yarn     --deploy-mode cluster  --files /home/dal_pro/dal_pro.keytab  --driver-memory  ${driver_memory_DPI_INFO_WB}  --num-executors  ${num_executors_DPI_INFO_WB}   --executor-memory  ${executor_memory_DPI_INFO_WB}   --executor-cores  ${executor_cores_DPI_INFO_WB}   --queue ${yarn_queue}  UIDSS-3.0-jar-with-dependencies.jar  Y_LoadRawData DPI_INFO_WB    ${hdfs_base_dir}/${wb_dir}/prov_id=${array_province[i]}/mon_id=${cur_month}/    0
  wait
  printf "External data ${array_province[i]}/${cur_month} is loaded\n"
done
