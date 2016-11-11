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

package cn.ctyun.UIDSS.hbase

import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.client.HTableInterface
import org.apache.hadoop.hbase.client.HConnectionManager
import org.apache.hadoop.hbase.client.Put
import org.apache.spark.SparkContext
import java.util.Properties
import org.apache.hadoop.hbase.util.Bytes
import cn.ctyun.UIDSS.hgraph.HGraphUtil
import cn.ctyun.UIDSS.utils.{ Utils, Logging }

object HBaseIO extends Logging {

  def getGraphTableRDD(sc: SparkContext, props: Properties): RDD[(ImmutableBytesWritable, Result)] = {
    val hconf = HBaseConfiguration.create()

    //set zookeeper quorum
    hconf.set("hbase.zookeeper.quorum", props.getProperty("hbaseZkIp"));
    //set zookeeper port
    hconf.set("hbase.zookeeper.property.clientPort", props.getProperty("hbaseZkPort"));
    hconf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    hconf.set("hbase.zookeeper.property.maxClientCnxns", props.getProperty("hbase_zookeeper_property_maxClientCnxns"));
    hconf.set("hbase.client.retries.number", props.getProperty("hbase_client_retries_number")); 
    
    hconf.addResource("./resources/core-site.xml")
    hconf.addResource("./resources/hbase-site.xml")
    hconf.addResource("./resources/hdfs-site.xml")

    //set which table to scan
    hconf.set(TableInputFormat.INPUT_TABLE, props.getProperty("hbaseTableName"))

    //println(getNowDate() + " ****** Start reading from HBase   ******")
    val rdd = sc.newAPIHadoopRDD(hconf, classOf[TableInputFormat], classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable], classOf[org.apache.hadoop.hbase.client.Result]).cache()
    //println(getNowDate() + " ****** Finished reading from HBase   ******")

    //遍历输出
    //    rdd.foreach {
    //      case (_, result) =>
    //        val key = Bytes.toString(result.getRow)
    //        //println("Row key:" + key)
    //        for (c <- result.rawCells()) {
    //          val dst = Bytes.toString(c.getQualifier)
    //          var value = 0
    //          try {
    //            value = Bytes.toInt(c.getValue)
    //          } catch {
    //            case _: Throwable =>
    //          }
    //          //println("        column is: " + dst + " ;  value is: " + value)
    //        }
    //    }
    rdd
  }

  def saveToGraphTable(sc: SparkContext, props: Properties, rddToSave: RDD[((String, String), String)]): Int = {

    //多分区并行输出
    rddToSave.foreachPartition {

      //一个分区内的所有行     
      case (rows) =>
        //println("        column is: " + this.getClass.getClassLoader().getResource(""))
        val hconf = HBaseConfiguration.create()

        //set zookeeper quorum
        hconf.set("hbase.zookeeper.quorum", props.getProperty("hbaseZkIp"));
        //set zookeeper port
        hconf.set("hbase.zookeeper.property.clientPort", props.getProperty("hbaseZkPort"));
        hconf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        hconf.set("hbase.zookeeper.property.maxClientCnxns", props.getProperty("hbase_zookeeper_property_maxClientCnxns"));
        hconf.set("hbase.client.retries.number", props.getProperty("hbase_client_retries_number")); 
    
        hconf.addResource("./resources/core-site.xml")
        hconf.addResource("./resources/hbase-site.xml")
        hconf.addResource("./resources/hdfs-site.xml")
        val connection = HConnectionManager.createConnection(hconf);
        val htable: HTableInterface = connection.getTable(props.getProperty("hbaseTableName"));

        //批量写入
        val flushInBatch = props.getProperty("flushInBatch")
        if (flushInBatch != null && "1".compareToIgnoreCase(flushInBatch) == 0) {
          htable.setAutoFlushTo(false);
          htable.setWriteBufferSize(1024 * 1024 * 16);
        }

        //println(getNowDate() + " ****** Start writing to HBase   ******")

        //var rowCount = 0 
        
        for (row <- rows.toArray) (
          {
            //row  ((行，列)，值）) 
            var src: String = row._1._1
            var dst: String = row._1._2
            var prop: Int = row._2.toInt
            //println("Row is: " + src + " ；column is: " + dst + " ; value is: " + prop)

            val put = new Put(Bytes.toBytes(src))
            put.add(HGraphUtil.COLUMN_FAMILY, Bytes.toBytes(dst), Bytes.toBytes(prop))
            put.setWriteToWAL(false)
            htable.put(put)
            
            //rowCount = rowCount +1
            //if ((rowCount % 1000)==0) { println(getNowDate() + " ****** Writing " + rowCount + " rows "  + src + " to " + dst + " to HBase ******")}
          })
        //println(getNowDate() + " ****** Finished writing to HBase   ******")  
        htable.flushCommits()
        htable.close();
        //println(getNowDate() + " ****** Flushed  to HBase   ******")

    }
    1
  }
}