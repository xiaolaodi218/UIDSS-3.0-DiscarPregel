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
import cn.ctyun.UIDSS.hgraph.{ HGraphUtil, GraphXUtil, HtoXGenUID, XtoHGenUID, XtoHGenUIDExt }
import cn.ctyun.UIDSS.hbase.HBaseIO
import cn.ctyun.UIDSS.graphxop.{ PregelGenUIDExtFindLinks, PregelGenUID }
import cn.ctyun.UIDSS.uidop.{ GenUID, GenUIDExt }

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
object GenUIDExtCmd extends Logging {

  def isDebugVert(vert: String): Boolean = {
    var bDebugVert = false
    if ("QQ974834277".compareTo(vert) == 0
      || "MN6678B8144976720525EFB8AF94FFB4C6".compareTo(vert) == 0
      || "MN83213DA70DCA118A608B49CB24D98665".compareTo(vert) == 0
      || "ME80830181509B6D9840769D4DA3254DFD".compareTo(vert) == 0
      || "IS81A6FA8B6704ACBDE33D2C44FE038613".compareTo(vert) == 0) {
      bDebugVert = true
    }
    bDebugVert
  }  
  
  
  def execute(sc: SparkContext, props: Properties): Unit = {

    /********  一 、从HBase中取出数据  *******/

    //Output:  RDD[(ImmutableBytesWritable, Result)]
    val rddHbase = HBaseIO.getGraphTableRDD(sc, props)
    info(getNowDate() + " ******  Finished loading  rows from HBase ******")

    //生成序号
    //为每个起始点加序列号sn，也就是HBase分区序号*每分区最大行数 + 该分区内的行序号。
    //比如： （100100001, {AI430851876/v:$1AN06F6642CE07804C26B847BEAEEB0204A/1458733523410/Put/vlen=4/mvcc=0} ) 
    info(" ****** 点序号  ******")
    val rddHBaseWithSN = rddHbase
      .mapPartitionsWithIndex { (ind, vs) =>
        var sn = GraphXUtil.MAX_VERTICE + ind * GraphXUtil.MAX_VERTICE_PER_PARTITION
        val lst = vs.map {
          case (_, v) =>
            sn = sn + 1
            val row = Bytes.toString(v.getRow)
            //if (isDebugVert(row)) {  println(row + "  is assigned sn of " + sn + " in partition " + ind) }
            (sn, v)
        }
        println("Partition  " + ind + " has  total sn of " + sn)
        lst
      }
    
    //再分区提高并行度
    val partitionNum = props.getProperty("rddPartitions").toInt
    val rdd = rddHBaseWithSN.repartition(partitionNum)
    rddHbase.unpersist(true)
    //info(" ******  Read " + rdd.count().toString() + " rows from HBase ******")

    /******** 二、生成图 *******/

    //Input: RDD[(ImmutableBytesWritable, Result)]
    //Output: Graph[(String, Long), (String, Int)]
    //---其中   Vertex (vid: Long, (typ: String, temp:Long))
    //---         Edge (src: Long, dst: Long, prop: (typ: String , weight: Int) )
    info(getNowDate() + " ******  从  RDD 生成图  ******")
    val graph = HtoXGenUID.getGraphRDD(rdd, sc)

    //Other option:
    //也可以用 graph.subgraph(epred, vpred) 过滤连接过多的节点

    //Other option: 
    //直接找到邻接节点
    //val cc = graph.connectedComponents().vertices
    //info(" ******  connected Components  ******" +cc.count() )

    /*不再需要，因为已经事先取出了邻接表
    //找出图中所有节点关联的UID节点,并记录到节点的UID属性中
    info(" ******  得到的相邻节点******")
    val graphVertexWithUID = PregelGenUIDExtFindLinks(graph)
    */

    /******** 三、找出图中所有的关联树,树名为树中节点的最小序号 *******/

    //Input: Graph[(String, Long), (String, Int)]
    //Output: Graph[(String, Long), (String, Int)]
    //---其中   
    //  Vertex (vid: Long, (typ: String, temp:Long))
    //  typ: 节点类型；temp: Long 记录了树中节点的最小序号  
    //  Edge (src: Long, dst: Long, prop: (typ: String , weight: Int) )
    //  typ: 连接类型；weight: Int 连接权重  
    
    //PregelGenUID.setVidDebug(vidDebug.value) //传点序号
    val searchDepth = props.getProperty("searchDepth").toInt
    info(getNowDate() + " ******  search depth is " + searchDepth + "  *******")
    val graphCnnd = PregelGenUID(graph, searchDepth)
    //info(getNowDate() +" ******  全图所有的节点，关联后状态 ******")
    //println("graphCnnd.vertices " + graphCnnd.vertices.collect().mkString("\n")) 

    /******** 四、按树名组合所有节点 *******/
    //Input: 
    //1.  Graph[(String, (Long, String)), (String, Long)]
    //---其中       
    //  Vertex (vid: Long, (typ: String, cvid:Long))
    //  vid: 节点序号; typ: 节点类型；cvid: Long 记录了树中节点的最小序号  
    //  Edge (src: Long, dst: Long, prop: (typ: String , weight: Int) )
    //  typ: 连接类型；weight: Int 连接权重  
    //2.  (vid: Long, (id: String, links: String))
    //  vid: 节点序号; id: id标识； links: 邻接表

    //节点序号与事先存储邻接表对应 
    // input :  
    // (vid:Long,  (id: String, links: String))
    // (vid:Long, (typ: String, cvid:Long))
    //output: 
    // (vid:Long, ((id: String, links: String),(typ: String, cvid:Long) ) ) 
    val rddLinksJoinVerts = HtoXGenUID.rddVidtoLinks.join(graphCnnd.vertices)
    //println("rddLinksJoinVerts " + rddLinksJoinVerts.collect().mkString("\n")) 

    //转为以最小序号为key的二元组
    //Input: 
    // (vId:Long, ((id: String, links: String),(typ: String, cvid:Long) ) ) 
    val rddCnndInId = rddLinksJoinVerts.map {
      case (vid, ((id, links), (typ, cvid))) => {
        //if (isDebugVert(id) ) { println("" + id + " has links" + links + "; cvid is " + cvid)    }
        (cvid, (id, links))
      }
    }
    //println("rddCnndInId " + rddCnndInId.collect().mkString("\n")) 

    //按最小序号分组
    //Input: 
    //  (cvid:Long, (id: String, links: String)) 
    //Output: 
    //  List[(id: String, links: String)]   即每个邻接树的所有节点保存为一个List
    //---其中   id: 是节点id  , neighbors: String 记录了相邻的节点
    //---比如 List((AI430851876, AN1;ID1), (IDb5eafeb6f2e8228df4c23fc2e4f1f0b2, AN1;AI430851876),...)
    info(getNowDate() + " ******  每个邻接树的所有节点保存为一个List ******")
    val rddCnndGroup = rddCnndInId.groupByKey().map { case (v) => v._2.toList }
    //println("rddCnndGroup is:  " +  rddCnndGroup.collect().mkString("\n"))

    /******** 五、UID生成 *******/
    //算出优势UID，如果没有则要生成
    //Output: RDD[(String, List[(String, String)])]   即每个邻接树的所有节点保存为一个List 
    //---其中 第一个String是找到或生成的这个树的UID，  （id: 是节点id  , uid: String 记录了相邻的UID）
    //---比如   UDc7e88a94542343fa83ff7a5b6c18c57e， List((AI430851876,), (IDb5eafeb6f2e8228df4c23fc2e4f1f0b2,),...)
    info(getNowDate() + " ****** 算出优势UID，如果没有则要生成   ******")
    val rddGroupWithUID = GenUIDExt(rddCnndGroup, props)
    //println("rddGroupWithUID is:  " + rddGroupWithUID.collect().mkString("\n"))

    /********六、计算出需要添加（更新）的UID边 *******/
    //记下所有要添加的（原来没有UID的节点，增加到），更新的（对应2条： 删除旧的[即添加权重为0的到旧]，添加新的[]）
    //Input: RDD[List[(id: String, uid: String)]]   即每个邻接树的所有节点保存为一个List. 其中 id: 是节点id  , uid: String 记录了相邻的UID
    //Output: RDD[((String, String), Long)]     ((行，列)，值）)
    //---   ((行，列)，值）)
    //---  ((IDb5eafeb6f2e8228df4c23fc2e4f1f0b2,zzUDc7e88a94542343fa83ff7a5b6c18c57e),1)
    info(getNowDate() + " ****** 计算出所有要添加的（原来没有UID的节点，增加到），更新的  ******")
    var rddNewRelations = XtoHGenUIDExt(rddGroupWithUID)
    val partNumHBaseO = props.getProperty("rddPartNumHBaseO").toInt
    if (partNumHBaseO > 0) {
      rddNewRelations = rddNewRelations.repartition(partNumHBaseO)
    }
    //println("rddNewRelations is:  " +  rddNewRelations.collect().mkString("\n"))

    /********七、保存UID边到HBase *******/
    //row  ((行，列)，值）)
    HBaseIO.saveToGraphTable(sc, props, rddNewRelations)
  }
}