/*********************************************************************
 * 
 * CHINA TELECOM CORPORATION CONFIDENTIAL
 * ____________________________________________________________
 * 
 *  [2015] - [2020] China Telecom Corporation Limited, 
 *  All Rights Reserved.
 * 
 * NOTICE:  All information contained herein is, and remains
 * the property of China Telecom Corporation and its suppliers,
 * if any. The intellectual and technical concepts contained 
 * herein are proprietary to China Telecom Corporation and its 
 * suppliers and may be covered by China and Foreign Patents,
 * patents in process, and are protected by trade secret  or 
 * copyright law. Dissemination of this information or 
 * reproduction of this material is strictly forbidden unless prior 
 * written permission is obtained from China Telecom Corporation.
 **********************************************************************/
package cn.ctyun.UIDSS.cmds

import java.util.Properties

import cn.ctyun.UIDSS.hbase.HBaseIO
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer
import cn.ctyun.UIDSS.utils.{Logging, Utils}
import cn.ctyun.UIDSS.hgraph.{GraphXUtil, HGraphUtil}

/**
 * 类描述：删除某个日期前的网络侧数据 (因为网络侧每次都是加载全量)
 *
 */
object DeleteOldDataCmd extends Logging {

  def execute(sc: SparkContext, props: Properties, oldTime: Long): Unit = {

    /********  一 、从HBase中取出数据  *******/
    //Output:  RDD[(ImmutableBytesWritable, Result)]
    val rddHbase = HBaseIO.getGraphTableRDD(sc, props)
    info(getNowDate() + " ******  Finished loading  rows from HBase ******")

    /********  二 、找出老数据  *******/
    val rddOldData = rddHbase.flatMap[((String, String), String)] {
      case (sn, v) => {
        //mapToEdges(v)
        val buf = new ListBuffer[((String, String), String)]

        val row = Bytes.toString(v.getRow.drop(2))
        val rowTy = row.substring(0, 2)

        var totalNonUID = 0

        for (c <- v.rawCells()) {
          val dst = Bytes.toString(c.getQualifier)
          val ty = dst.substring(0, 2)

          var value = 0
          try {
            value = Bytes.toInt(c.getValue)
          } catch {
            case _: Throwable =>
          }
          
          if (value > 0) {
            //只删除网络侧数据
            if (ty.compareTo(HGraphUtil.STR_TABLE_UID_FIX_DPI_USER_ACCOUNT) == 0
              || ty.compareTo(HGraphUtil.STR_TABLE_UID_OTH_DPI_USER_ACCOUNT) == 0) {
              val timestamp = c.getTimestamp
              if (timestamp < oldTime) {
                buf += (((row, dst), "0"))
              }
            }

            //记录下非UID关联数
            if (ty.compareTo(HGraphUtil.STR_TABLE_UID) != 0) {
              totalNonUID += 1
            }
          }
        }

        //非UID行，没有非UID关联的孤点
        if (rowTy.compareTo(HGraphUtil.STR_UD) != 0 && totalNonUID == 0) {
          //删除残留的UID关联
          for (c <- v.rawCells()) {
            val dst = Bytes.toString(c.getQualifier)
            val ty = dst.substring(0, 2)

            var value = 0
            try {
              value = Bytes.toInt(c.getValue)
            } catch {
              case _: Throwable =>
            }

            //把到UID的关联双向删除
            if (value > 0 && ty.compareTo(HGraphUtil.STR_TABLE_UID) == 0) {
              buf += (((row, dst), "0"))
              buf += (((dst.substring(2), ty + row), "0"))
            }

          }
        }

        buf.toIterable
      }
    }
    info(getNowDate() + " ******  Got old edges. ******")

    /********三、把老数据的权重置为0,写回到HBase *******/
    //row  ((行，列)，值）)
    HBaseIO.saveToGraphTable(sc, props, rddOldData)
    info(getNowDate() + " ******  Finished writing rows from HBase ******")
  }
}