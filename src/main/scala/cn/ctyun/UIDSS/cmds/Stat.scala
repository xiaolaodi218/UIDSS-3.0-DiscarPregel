/**
 * *******************************************************************
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
 * ********************************************************************
 */
package cn.ctyun.UIDSS.cmds

import java.util.Properties
import java.util.UUID

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.client.HTableInterface
import org.apache.hadoop.hbase.client.HConnectionManager
import org.apache.hadoop.hbase.client.Put
import cn.ctyun.UIDSS.utils.{Logging, Utils}
import cn.ctyun.UIDSS.hgraph.{GraphXUtil, HGraphUtil, HtoXGenUID, XtoHGenUID, XtoHGenUIDExt}
import cn.ctyun.UIDSS.graphxop.{PregelGenUID, PregelGenUIDExtFindLinks}
import cn.ctyun.UIDSS.hbase.HBaseIO
import cn.ctyun.UIDSS.uidop.{GenUID, GenUIDExt}

/**
 * 类描述：统计UID生成的结果
 * 1.UID到其它标识数据的关联,按各种关联量汇总
 * 2.其它标识数据到UID的关联,按各种关联量汇总,
 */
object Stat extends Logging {

  var iLargeNode = 1000

  def execute(sc: SparkContext, props: Properties, path: String) = {

    val hdfsPath = props.getProperty("hdfs")
    val largeNode = props.getProperty("largeNodeSize")
    if (largeNode != null && largeNode.length() > 0 && largeNode.toInt > 0) {
      iLargeNode = largeNode.toInt
    }
    
    //从HBase中取出数据
    info(getNowDate() + " ******  Loading  rows from HBase ******")
    val rddHbase = HBaseIO.getGraphTableRDD(sc, props)

    //每个节点,算出分类计数
    val rddCount = rddHbase.map {
      case (_, v) => mapToCount(v)
    }
    //所有节点,按类型汇总
    val rddStat = rddCount.reduceByKey {
      case (x, y) => x + y
    }
    //写入HDFS
    rddStat.coalesce(1).sortByKey().saveAsTextFile(hdfsPath + "/" + path + "/stat-" + getNowDateShort())
    //rddStat.coalesce(1).sortByKey().saveAsTextFile("stat-" + getNowDateShort() )    

    //每个节点,算出总计数
    val rddCountAll = rddHbase.map {
      case (_, v) => mapToCountA(v)
    }
    //所有节点,按类型汇总
    val rddStatAll = rddCountAll.reduceByKey {
      case (x, y) => x + y
    }
    //写入HDFS
    rddStatAll.coalesce(1).sortByKey().saveAsTextFile(hdfsPath + "/" + path + "/stat-all-" + getNowDateShort())

    //列出所有大节点
//    val rddLargeNodes = rddHbase.flatMap {
//      case (_,v) => isLargeNode(v)
//    }
//    //写入HDFS
//    rddLargeNodes.coalesce(1).sortByKey().saveAsTextFile(hdfsPath + "/" + path + "/stat-large-node-" + getNowDateShort())

  }

  //把HBase一行转换成多条边，目标节点id, (源节点sn, 连接类型)）
  //可以在这一步进行边的过滤
  //其它节点只统计个数直接
  def mapToCount(v: Result) = {

    var countUD = 0
    var countMN = 0
    var countWN = 0
    var countAN = 0
    var countQQ = 0

    //节点类型
    val ty = (Bytes.toString(v.getRow.drop(2))).substring(0, 2)

    //所有边按类型计数
    for (c <- v.rawCells()) {
      var dst = Bytes.toString(c.getQualifier)
      dst = dst.substring(2, 4)
      dst match {
        case "UD" => countUD += 1
        case "MN" => countMN += 1
        case "WN" => countWN += 1
        case "AN" => countAN += 1
        case "QQ" => countQQ += 1
        case _    =>
      }
    }

    val strCountUD = if (countUD>99) "99" else (100 + countUD % 100).toString().substring(1, 3)
    val strCountMN =if (countMN>99) "99" else (100 + countMN % 100).toString().substring(1, 3)
    val strCountWN =if (countWN>99) "99" else (100 + countWN % 100).toString().substring(1, 3)
    val strCountAN = if (countAN>99) "99" else (100 + countAN % 100).toString().substring(1, 3)
    val strCountQQ = if (countQQ>99) "99" else (100 + countQQ % 100).toString().substring(1, 3)

    var key = ty + strCountUD + strCountMN + strCountWN + strCountAN + strCountQQ

    (key, 1)
  }

  def mapToCountA(v: Result) = {
    //节点类型
    val ty = (Bytes.toString(v.getRow.drop(2))).substring(0, 2)
    //所有边按类型计数
    val iNeighbors = v.rawCells().size
    val count = if (iNeighbors>99)  (iNeighbors/100)*100 else iNeighbors
    val key = ty + count.toString()
    (key, 1)
  }

  def isLargeNode(v: Result): Iterable[(String, Long)] = {

    val buf = new ListBuffer[(String, Long)]

    //超大节点暂不考虑
    val nodeSize = v.rawCells().size
    if (nodeSize > iLargeNode) {
      buf += ((Bytes.toString(v.getRow),nodeSize))
    }
    buf.toIterable
  }
}