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

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.util.Bytes
import cn.ctyun.UIDSS.utils.{Logging, Utils}
import cn.ctyun.UIDSS.hgraph.{GraphXUtil, HGraphUtil, HtoXBatchQuery}
import cn.ctyun.UIDSS.graphxop.PregelBatchQuery
import cn.ctyun.UIDSS.hbase.HBaseIO

object BatchQueryCmd {

  def execute(sc: SparkContext, props: Properties): Unit = {
    
    //从HBase中取出数据
    //Output:  RDD[(ImmutableBytesWritable, Result)]
    val rdd = HBaseIO.getGraphTableRDD(sc, props)

    //生成图
    //Input: RDD[(ImmutableBytesWritable, Result)]
    //Output: Graph[(String, (Long, String)), (String, Long)]
    //---其中  Vertex (vid: VertextId: Long, (id: String, temp:Long ))
    //---        Edge (src: VertexId, dst: VertexId, prop: (type: String , weight: Long) )
    val graph = HtoXBatchQuery.getGraphRDD(rdd)

    // Count all users 
    //val vcount = graph.vertices.count
    //println("total v: " + vcount)

    // Count all the edges where src > dst
    //val vcount = graph.edges.filter(e => e.srcId > e.dstId).count  
    //val ecount = graph.edges.count  
    //println("total e: " + ecount)

    val graphCnnd = PregelBatchQuery(graph)

    //按树名组合所有节点
    val rddCnndInId = graphCnnd.vertices.map {
      case (vertexId, (id, cc)) => (cc, id)
    }
    //println(rddCnndInId.collect().mkString("\n"))
    val rddCnndGroup = rddCnndInId.groupByKey().map { case (v) => v._2.toList }
    //println(rddCnndGroup.collect().mkString("\n"))

    //输出UID批量查询结果
    //rddCnndGroup.saveAsTextFile("hdfs://centos6-160:9000/uid/result3")
  }
}