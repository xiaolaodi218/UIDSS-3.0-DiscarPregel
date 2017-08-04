/**
 * *******************************************************************
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
 * ********************************************************************
 */
package cn.ctyun.UIDSS.cmds

import java.util.Properties
import java.util.UUID
import org.apache.spark.SparkContext

import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel

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
import cn.ctyun.UIDSS.graphxop.{ PregelGenUIDFindPairs, PregelGenUIDFindGroups }
import cn.ctyun.UIDSS.uidop.{ GenUIDExtPair, GenUIDExtGroup }

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
object GenUID3Cmd extends Logging {

  def execute(sc: SparkContext, props: Properties, path: String): Unit = {

    val hdfsPath = props.getProperty("hdfs")
    val recordLargeGroup = props.getProperty("recordLargeGroup")

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
            val row = Bytes.toString(v.getRow.drop(2))
            //if (isDebugVert(row)) {  println(row + "  is assigned sn of " + sn + " in partition " + ind) }
            (sn, v)
        }
        lst
      }

    //再分区提高并行度
    val partitionNum = props.getProperty("rddPartitions").toInt
    val rdd = rddHBaseWithSN.repartition(partitionNum)
    rddHbase.unpersist(true)
    //HBase分片大小并不均匀, 强制先进行repartition工作,避免与之后阶段工作一起进行,单个单个节点负载过重.
    val cnt = rdd.count().toString()
    info(" ******  Read " + cnt + " rows from HBase ******")
    println(" ******  Read " + cnt + " rows from HBase ******")

    /******** 二、生成图 *******/
    //Input: RDD[(ImmutableBytesWritable, Result)]
    //Output: Graph[(String, Long), (String, Int)]
    //---其中   Vertex (vid: Long, (typ: String, temp:Long))
    //---         Edge (src: Long, dst: Long, prop: (typ: String , weight: Int) )
    info(getNowDate() + " ******  从  RDD 生成图  ******")
    val graph = HtoXGenUID.getGraphRDD(rdd, sc)



    /******** 三、找出图中所有的号码对及孤立号码 *******/
    //Input: Graph[(String, Long), (String, Int)]
    //Output: Graph[(String, Long), (String, Int)]
    //---其中   
    //  Vertex (vid: Long, (typ: String, temp:Long))
    //  typ: 节点类型；temp: Long 记录了树中节点的最小序号  
    //  Edge (src: Long, dst: Long, prop: (typ: String , weight: Int) )
    //  typ: 连接类型；weight: Int 连接权重  

    val searchDepth = props.getProperty("searchDepth").toInt
    info(getNowDate() + " ******  search depth is " + searchDepth + "  *******")
    //val graphCnnd = PregelGenUID(graph, searchDepth, props)
    val graphCnnd = PregelGenUIDFindPairs(graph, 3, props)
    //info(getNowDate() +" ******  全图所有的节点，关联后状态 ******")
    println("graphCnnd.vertices: \n" + graphCnnd.vertices.collect().mkString("\n")) 

    //input:
    // vert:   (VertexId,(String, (Long, List[(Long, Long)])))
    //(1004520000000001,(CI,(0,List((1003750000000001,1000510000000001), (1003830000000001,1000510000000001), (1000510000000001,1000510000000001)))))
    //(1002150000000001,(UD,(0,List((1003850000000001,1002700000000001), (1002700000000001,1002700000000001)))))
    //output:/
    //(1002150000000001,(UD,(1003850000000001)))
    //(1002150000000001,(UD,(1002700000000001)))
    var rddDirectedRawCNPairs = graphCnnd.vertices.flatMap { vert =>
      {
        var verts = new ListBuffer[((Long,Long), Int)]
        if (HGraphUtil.STR_ACCS_NUM.compareTo(vert._2._1) == 0
          || HGraphUtil.STR_MBL_NUM.compareTo(vert._2._1) == 0
          || HGraphUtil.STR_WB_NUM.compareTo(vert._2._1) == 0) { //通信号码节点

          val neighbors = vert._2._2._2
          if (neighbors.length == 0) { //孤立的号码
            verts += (((vert._1, 0L), 1))
          } 
          else { //有邻居的号码
            for (neighbor <- neighbors) { verts += (((vert._1 min neighbor._1, vert._1 max neighbor._1),  1)) } //号码对,并排序
          }
        }
        verts.toIterable
      }
    }
    println("rddDirectedRawCNPairs: \n " + rddDirectedRawCNPairs.collect().mkString("\n")) 
    
    //号码对合并重复的,出现次数相加
     rddDirectedRawCNPairs = rddDirectedRawCNPairs.reduceByKey {
      case (weight1, weight2) => { //双向都会出现（应该是出现两次），合并重复的
         weight1 + weight2 
      }
    }
    println("rddDirectedRawCNPairs merged " + rddDirectedRawCNPairs.collect().mkString("\n"))      
     
    val rddRawCNPairs = rddDirectedRawCNPairs.flatMap {
      case (((sn1, sn2), weight)) => {
        val buf = new ListBuffer[(Long, Long)]
        //为了过滤,双向都有才保留
        if (weight > 1) { //号码对
          //调试用
          buf += ((sn1, sn2))
        }
        buf.toIterable
      }
    }

    println("rddRawCNPairs: \n " + rddRawCNPairs.collect().mkString("\n"))          
    
    //孤立的号码
    val rddLonelyCN =   rddDirectedRawCNPairs.flatMap {
      case (((sn1, sn2), weight)) => {
        val buf = new ListBuffer[Long]
        if (weight== 1 && sn1==sn2) { //号码对
          //调试用
          buf += (sn1)
        }
        buf.toIterable
      }      
    }

     println("rddLonelyCN: \n " + rddLonelyCN.collect().mkString("\n"))             
   
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
    
    
    //为起点找到编码和所有邻接点
    val rddRawCNPairsJoinF = HtoXGenUID.rddVidtoLinks.join(rddRawCNPairs)
    println("rddRawCNPairsJoinF " + rddRawCNPairsJoinF.collect().mkString("\n")) 

    //Input: 
    // (vId:Long, ((id: String, links: String),(typ: String, cvid:Long) ) ) 
    val rddRawCNPairsWithLinksF = rddRawCNPairsJoinF.map {
      case (s_vid, ((id, links), e_vid)) => {
        (e_vid, ((s_vid,id), links))
      }
    }

     //为终点找到编码和所有邻接点
    val rddRawCNPairsJoinS = HtoXGenUID.rddVidtoLinks.join(rddRawCNPairsWithLinksF)


    val rddRawCNPairsWithLinks = rddRawCNPairsJoinS.map {
      case (e_vid, ((e_id, e_links), ((s_vid, s_id),s_links))) => {
        (((s_vid,s_id),s_links),((e_vid,e_id), e_links))
      }
    }
    println("rddRawCNPairsWithLinks " + rddRawCNPairsWithLinks.collect().mkString("\n")) 
    
    //判断是否是属于同一用户的号码对
    //判断是否同一人的号码对，建立号码之间关联  （按号码对取出子图 （）， 返回一对或2对1个号码（没有匹配上）graph的判断号码对算法）     （一对（1个），或直接一个号码）
    val rddRealPairs = GenUIDExtPair(rddRawCNPairsWithLinks, props)
    println("rddRealPairs " + rddRealPairs.collect().mkString("\n"))     

    //号码之间的关联， 建立graphx图.   号码是点，号码对是边。 
    var rddEdge = rddRealPairs.flatMap{ case (((s_vid,s_id),s_links),((e_vid,e_id), e_links)) => {
        val buf = new ListBuffer[Edge[(String, Int)]]
        if (e_vid>0) {
          buf += Edge(s_vid, e_vid, ("PR", 1))
        }
        buf.toIterable
      }    
    }
    
    val rddVertex = rddRealPairs.flatMap{ case (((s_vid,s_id),s_links),((e_vid,e_id), e_links)) => {
        val buf = new ListBuffer[(Long, (String,  (Long, List[(Long, Long)])))]
        val neighbors : List[(Long, Long)]= List()
        if (e_vid>0) {
          buf += ((s_vid, (s_id, (0L,neighbors))))
          buf += ((e_vid, (e_id, (0L,neighbors)))) 
        } else {
          buf += ((s_vid, (s_id, (0L,neighbors))))
        }
          buf.toIterable
        }
      }    
    
    
     val graphPairs = Graph(rddVertex, rddEdge, null, StorageLevel.MEMORY_AND_DISK, StorageLevel.MEMORY_AND_DISK)    
    //	pregel 获得 关联的组
    val graphGroup = PregelGenUIDFindGroups(graphPairs, 3, props)
//  
    //节点序号与事先存储邻接表对应 
    // input :  
    // (vid:Long,  (id: String, links: String))
    // (vid:Long, (typ: String, cvid:Long))
    //output: 
    // (vid:Long, ((id: String, links: String),(typ: String, cvid:Long) ) ) 
    
    // vert:   (VertexId,(String, (Long, List[(Long, Long)])))
    val rddVerticesExt = graphGroup.vertices.flatMap { vert =>
      {
        var verts = new ListBuffer[(Long, (String, Long))]
        val neighbors = vert._2._2._2
        if (neighbors.length == 0) {
          verts += ((vert._1, (vert._2._1, vert._2._2._1)))
        } 
        else { //需要扩展的节点,比如宽带节点
          for (neighbor <- neighbors) { verts += ((vert._1, (vert._2._1, neighbor._2))) }
        }
        verts.toIterable
      }
    }
    
    val rddLinksJoinVerts = HtoXGenUID.rddVidtoLinks.join(rddVerticesExt)
    println("rddLinksJoinVerts " + rddLinksJoinVerts.collect().mkString("\n")) 

    //转为以最小序号为key的二元组
    //Input: 
    // (vId:Long, ((id: String, links: String),(typ: String, cvid:Long) ) ) 
    val rddCnndInId = rddLinksJoinVerts.map {
      case (vid, ((id, links), (typ, cvid))) => {
        //if (isDebugVert(id) ) { println("" + id + " has links" + links + "; cvid is " + cvid)    }
        (cvid, (id, links))
      }
    }
    
    
    val rddCnndGroup = rddCnndInId.groupByKey().map { case (v) => v._2.toList }
     println("rddCnndGroup is:  " + rddCnndGroup.collect().mkString("\n"))
    
    /******** 五、UID生成 *******/
    //	把所有的组的信息 组成图。
    //算出优势UID，如果没有则要生成
    //Output: RDD[(String, List[(String, String)])]   即每个邻接树的所有节点保存为一个List 
    //---其中 第一个String是找到或生成的这个树的UID，  （id: 是节点id  , uid: String 记录了相邻的UID）
    //---比如   UDc7e88a94542343fa83ff7a5b6c18c57e， List((AI430851876,), (IDb5eafeb6f2e8228df4c23fc2e4f1f0b2,),...)
    info(getNowDate() + " ****** 算出优势UID，如果没有则要生成   ******")
    val rddGroupWithUID = GenUIDExtGroup(rddCnndGroup, props)
    //println("rddGroupWithUID is:  " + rddGroupWithUID.collect().mkString("\n"))
    

    /********六、计算出需要添加（更新）的UID边 *******/
    //记下所有要添加的（原来没有UID的节点，增加到），更新的（对应2条： 删除旧的[即添加权重为0的到旧]，添加新的[]）
    //Input: RDD[List[(id: String, uid: String)]]   即每个邻接树的所有节点保存为一个List. 其中 id: 是节点id  , uid: String 记录了相邻的UID
    //Output: RDD[((String, String), Long)]     ((行，列)，值）)
    //---   ((行，列)，值）)
    //---  ((IDb5eafeb6f2e8228df4c23fc2e4f1f0b2,zzUDc7e88a94542343fa83ff7a5b6c18c57e),1)
    info(getNowDate() + " ****** 计算出所有要添加的（原来没有UID的节点，增加到），更新的  ******")
    var rddNewRelations = XtoHGenUIDExt(rddGroupWithUID)
    //println("rddNewRelations is:  " +  rddNewRelations.collect().mkString("\n"))

    /********七、保存UID边到HBase *******/
    //row  ((行，列)，值）)
    HBaseIO.saveToGraphTable(sc, props, rddNewRelations)
  }
}