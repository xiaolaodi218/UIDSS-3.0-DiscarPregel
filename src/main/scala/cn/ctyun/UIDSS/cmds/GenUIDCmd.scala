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
import cn.ctyun.UIDSS.utils.{ Utils, Logging }
import cn.ctyun.UIDSS.hgraph.{ HGraphUtil, GraphXUtil, HtoXGenUID, XtoHGenUID }
import cn.ctyun.UIDSS.graphxop.{ PregelGenUIDFindLinks, PregelGenUID }
import cn.ctyun.UIDSS.uidop.GenUID

/**
 * 类描述：生成UID操作
 * 1. 生成图时, 需要把关联的UID写入节点的属性
 * 2. 遍历生成关联树后,找出优势UID
 * 3. 更新所有关联节点
 *
 *  需要改节点结构，String用来记录UID
 * 一次广播先为所有节点找到相邻UID。 需要广播UID（初始步骤）， 接收，保存
 * 下次广播找到相邻树
 * 生成包含每个节点的UID信息的相邻树RDD
 * 算出优势UID，
 * 记下所有要添加的（原来没有UID的节点，增加到），更新的（对应2条： 删除旧的[即添加权重为0的到旧]，添加新的[]）
 * 保存到数据库
 */
object GenUIDCmd {

  def execute(sc: SparkContext, props: Properties): Unit = {

//    //从HBase中取出数据
//    //Output:  RDD[(ImmutableBytesWritable, Result)]
//    val rdd = HBaseIO.getGraphTableRDD(sc, props)
//
//    //生成图
//    //Input: RDD[(ImmutableBytesWritable, Result)]
//    //Output: Graph[(String, (Long, String)), (String, Long)]
//    //---其中   Vertex (vid: VertextId: Long, (id: String, (temp:Long, uid: String)))
//    //---         Edge (src: VertexId, dst: VertexId, prop: (type: String , weight: Long) )
//    val graph = HtoXGenUID.getGraphRDD(rdd)
//
//    //找出图中所有节点关联的UID节点,并记录到节点的UID属性中
//    //Input: Graph[(String, (Long, String)), (String, Long)]
//    //Output: Graph[(String, (Long, String)), (String, Long)]
//    //---其中  Vertex (vid: VertextId: Long, (id: String, (temp:Long, uid: String)))
//    //  uid: String 记录了相邻的UID
//    val graphVertexWithUID = PregelGenUIDFindLinks(graph)
//
//    //找出图中所有的关联树,树名为树中节点的最小序号  
//    //Input: Graph[(String, (Long, String)), (String, Long)]
//    //Output: Graph[(String, (Long, String)), (String, Long)]
//    //---其中  Vertex (vid: VertextId: Long, (id: String, (temp:Long, uid: String)))
//    //  temp: Long 记录了树中节点的最小序号  
//    val graphCnnd = PregelGenUID(graphVertexWithUID)
//
//    //按树名组合所有节点
//    //Input: Graph[(String, (Long, String)), (String, Long)]
//    //---其中  Vertex (vid: VertextId: Long, (id: String, (cc:Long, uid: String)))
//    //  cc: Long 记录了树中节点的最小序号  , uid: String 记录了相邻的UID
//    //Output: RDD[(cc:Long, (id: String, uid: String))]
//    //---其中   cc: Long 记录了树中节点的最小序号, id: 是节点id  , uid: String 记录了相邻的UID
//    val rddCnndInId = graphCnnd.vertices.map {
//      case (vertexId, (id, (cc, uid))) => (cc, (id, uid))
//    }
//    //Output: RDD[List[(id: String, uid: String)]]   即每个邻接树的所有节点保存为一个List
//    //---其中   id: 是节点id  , uid: String 记录了相邻的UID
//    //---比如 List((AI430851876,), (IDb5eafeb6f2e8228df4c23fc2e4f1f0b2,),...)
//    val rddCnndGroup = rddCnndInId.groupByKey().map { case (v) => v._2.toList }
//    //println(rddCnndGroup.collect().mkString("\n"))
//
//    //UID生成
//    //算出优势UID，如果没有则要生成
//    //Output: RDD[(String, List[(String, String)])]   即每个邻接树的所有节点保存为一个List 
//    //---其中 第一个String是找到或生成的这个树的UID，  （id: 是节点id  , uid: String 记录了相邻的UID）
//    //---比如   UDc7e88a94542343fa83ff7a5b6c18c57e， List((AI430851876,), (IDb5eafeb6f2e8228df4c23fc2e4f1f0b2,),...)
//    val rddGroupWithUID = GenUID(rddCnndGroup, props)
//
//    //记下所有要添加的（原来没有UID的节点，增加到），更新的（对应2条： 删除旧的[即添加权重为0的到旧]，添加新的[]）
//    //Output: RDD[((String, String), Long)]     ((行，列)，值）)
//    //---   ((行，列)，值）)
//    //---  ((IDb5eafeb6f2e8228df4c23fc2e4f1f0b2,zzUDc7e88a94542343fa83ff7a5b6c18c57e),1)
//    val rddNewRelations = XtoHGenUID(rddGroupWithUID)
//    //println(rddNewRelations.collect().mkString("\n"))
//
//    //保存UID生成结果到HBase
//    //row  ((行，列)，值）)
//    HBaseIO.saveToGraphTable(sc, props, rddNewRelations)
  }
}