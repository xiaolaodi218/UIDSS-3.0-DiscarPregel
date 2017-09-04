/*********************************************************************
 * 
 * CHINA TELECOM CORPORATION CONFIDENTIAL
 * ______________________________________________________________
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

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import cn.ctyun.UIDSS.utils.{ Utils, Logging }
import cn.ctyun.UIDSS.dfstohbase._
import cn.ctyun.UIDSS.hbase._

/**
 * 类描述：从HDFS加载关联关系到HBase
 *
 */
object LoadRawDataCmd extends Logging{

  def execute(sc: SparkContext, props: Properties, tableName: String, newData: String, oldData: String, lant_id: String): Unit = {
    val hdfsPath = props.getProperty("hdfs")
    tableName match {
      case "UID_INFO_MBL" => {
         info("Calling UIDInfoMBL. . hdfspath is: " + hdfsPath + newData + " lant_id is " + lant_id)       
        val rddNew = UIDInfoMBL(sc, hdfsPath + newData, "1", lant_id)
        //println("rddNewRelations is:  \n" + rddNew.collect().mkString("\n"))

        if (!oldData.isEmpty() && oldData.compareToIgnoreCase("0")!=0) {
          val rddOld = UIDInfoMBL(sc, hdfsPath + oldData, "1", lant_id)
          //println("rddOldRelations is:  \n" + rddOld.collect().mkString("\n"))

          val rddDiff = FindDiff(rddOld, rddNew)

          //保存UID生成结果到HBase
          //row  ((行，列)，值）)
          HBaseIO.saveToGraphTable(sc, props, rddDiff)
        } else {
          info("Writing UID_INFO_MBL records to Graph table. ")
          HBaseIO.saveToGraphTable(sc, props, rddNew)
        }
      }
      case "UID_INFO_WB" => {
        info("Calling UIDInfoWB. hdfspath is: " + hdfsPath + newData + " lant_id is " + lant_id)
        val rddNew = UIDInfoWB(sc, hdfsPath + newData, "1", lant_id)
        //println("rddNewRelations is:  \n" + rddNew.collect().mkString("\n"))

        if (!oldData.isEmpty() && oldData.compareToIgnoreCase("0")!=0) {
          //再分区提高并行度
          val rddNewRepart= rddNew.repartition(30)
          val rddOld = UIDInfoWB(sc, hdfsPath + oldData, "1", lant_id).repartition(30)
          //println("rddOldRelations is:  \n" + rddOld.collect().mkString("\n"))
//          val cntOld = rddOld.count()
//          info(" ******  Read " + cntOld + " rows from HBase ******")       
         
          var rddDiff = FindDiff(rddOld, rddNewRepart)
          
          //保存UID生成结果到HBase
          //row  ((行，列)，值）)
          HBaseIO.saveToGraphTable(sc, props, rddDiff)
        } else {
          info("Writing UID_INFO_WB records to Graph table. ")
          HBaseIO.saveToGraphTable(sc, props, rddNew)
        }
      }
      case "UID_INFO_TEL" => {
        info("Calling UIDInfoTEL. . hdfspath is: " + hdfsPath + newData + " lant_id is " + lant_id)
        val rddNew = UIDInfoTEL(sc, hdfsPath + newData, "1", lant_id)
        //println("rddNewRelations is:  \n" + rddNew.collect().mkString("\n"))

        if (!oldData.isEmpty() && oldData.compareToIgnoreCase("0")!=0 ) {
          //再分区提高并行度
          val rddNewRepart= rddNew.repartition(30)
          val rddOld = UIDInfoTEL(sc, hdfsPath + oldData, "1", lant_id).repartition(30)
          //println("rddOldRelations is:  \n" + rddOld.collect().mkString("\n"))
          
          var rddDiff = FindDiff(rddOld, rddNewRepart)

          //保存UID生成结果到HBase
          //row  ((行，列)，值）)
          HBaseIO.saveToGraphTable(sc, props, rddDiff)
        } else {
          info("Writing UID_INFO_TEL records to Graph table. ")
          HBaseIO.saveToGraphTable(sc, props, rddNew)
        }
      }
      //删除过期的可以用HBase脚本，删除3个月内没有更新的，update连接权重 = 0
      case "DPI_INFO_MBL" => {
        val rddNew = DPIInfoMBL(sc, hdfsPath + newData, "1")
        //保存UID生成结果到HBase
        //row  ((行，列)，值）)
        info("Writing DPI_INFO_MBL records to Graph table. ")
        HBaseIO.saveToGraphTable(sc, props, rddNew)
      }
      case "DPI_INFO_WB" => {
        val rddNew = DPIInfoWB(sc, hdfsPath + newData, "1")
        //保存UID生成结果到HBase
        //row  ((行，列)，值）)
        info("Writing DPI_INFO_WB records to Graph table. ")
        HBaseIO.saveToGraphTable(sc, props, rddNew)
      }
      case "DPI_RAW" => {
        val rddNew = DPIRaw(sc, hdfsPath + newData, "1")
        //println("rddNewRelations is:  \n" + rddNew.collect().mkString("\n"))
      }
      case _ => println("Not a valid table name!")
    }

  }
}