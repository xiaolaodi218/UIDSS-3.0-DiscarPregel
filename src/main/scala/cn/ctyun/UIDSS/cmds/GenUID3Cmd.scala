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

import cn.ctyun.UIDSS.UIDSS.info
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
import cn.ctyun.UIDSS.utils.{Logging, Utils}
import cn.ctyun.UIDSS.hgraph.{GraphXUtil, HGraphUtil, HtoXGenUID, XtoHGenUID, XtoHGenUIDExt}
import cn.ctyun.UIDSS.graphxop.{PregelGenUIDFindGroups, PregelGenUIDFindPairs}
import cn.ctyun.UIDSS.hbase.HBaseIO
import cn.ctyun.UIDSS.uidop.{GenUIDExtGroup, GenUIDExtPair}

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
    info("this is the getGraphTableRDD place ")
    val rddHbase = HBaseIO.getGraphTableRDD(sc, props)
    info(getNowDate() + " ******  Finished loading  rows from HBase ******")

    //生成序号
    //为每个起始点加序列号sn，也就是HBase分区序号*每分区最大行数 + 该分区内的行序号。
    //比如： （100100001, {AI430851876/v:$1AN06F6642CE07804C26B847BEAEEB0204A/1458733523410/Put/vlen=4/mvcc=0} ) 
    info(" ****** 点序号  ******")
    info("this is the rddHBaseWithSN place ")
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

    /******** 二、生成图 *******/
    //从HBase邻接表,转为GraphX的图
    //Input: RDD[(ImmutableBytesWritable, Result)]
    //Output: Graph[(String, (Long, List[(Long, Long)])), (String, Int)]
    //---其中   Vertex (vid: Long, (typ: String, temp:Long))
    //---         Edge (src: Long, dst: Long, prop: (typ: String , weight: Int) )
    info(getNowDate() + " ******  从  RDD 生成图  ******")
    info("this is the getGraphRDD place ")
    val graph = HtoXGenUID.getGraphRDD(rdd, sc)


    /******** 三、找出图中所有的号码对及孤立号码 *******/
    //GraphX的图，通过2层pregel找出相邻号码
    info("this is the graphCnnd place ")
    val searchDepth = props.getProperty("searchDepth").toInt
    val graphCnnd = PregelGenUIDFindPairs(graph, 2, props)
    //println("graphCnnd.vertices: \n" + graphCnnd.vertices.collect().mkString("\n")) 

    //把GraphX的图中节点的邻接表展开为号码对
    //input:
    // vert:   (VertexId,(String, (Long, List[(Long, Long)])))
    //(1004520000000001,(CI,(0,List((1003750000000001,1000510000000001), (1003830000000001,1000510000000001), (1000510000000001,1000510000000001)))))
    //(1002150000000001,(UD,(0,List((1003850000000001,1002700000000001), (1002700000000001,1002700000000001)))))
    //output:/
    //((1000960000000001,0),1)
    //((1002700000000001,1003750000000001),1)
    //((1002700000000001,1003830000000001),1)
    //((1002700000000001,1003850000000001),1)
    //((1002700000000001,1002700000000001),1)
    //((1000140000000001,0),1)
    var rddDirectedRawCNPairs = graphCnnd.vertices.flatMap { vert =>
      {
        var verts = new ListBuffer[((Long,Long), Int)]
        if (HGraphUtil.STR_ACCS_NUM.compareTo(vert._2._1) == 0
          || HGraphUtil.STR_MBL_NUM.compareTo(vert._2._1) == 0
          || HGraphUtil.STR_WB_NUM.compareTo(vert._2._1) == 0) { //通信号码节点
          val neighbors = vert._2._2._2
          if (neighbors.length < 2) { //孤立的号码
            verts += (((vert._1, 0L), 1))  //只有一个虚拟号码对
          } 
          else { //有邻居的号码
            for (neighbor <- neighbors) { verts += (((vert._1 min neighbor._1, vert._1 max neighbor._1),  1)) } //号码对展开,并把号码对里的两个号码排序
          }
        }
        verts.toIterable
      }
    }
    //println("rddDirectedRawCNPairs: \n " + rddDirectedRawCNPairs.collect().mkString("\n")) 
    
    //合并重复的号码对,出现次数相加
    //output:
    //((1000490000000001,0),1)  孤立节点自己到自己的边
    //((1000510000000001,1000510000000001),1)   非孤立节点自己到自己的边
    //((1002700000000001,1003850000000001),2)    非孤立节点之间成对的边
     rddDirectedRawCNPairs = rddDirectedRawCNPairs.reduceByKey {
      case (weight1, weight2) => { //双向都会出现（应该是出现两次），合并重复的
         weight1 + weight2 
      }
    }
    //println("rddDirectedRawCNPairs merged " + rddDirectedRawCNPairs.collect().mkString("\n"))      
    
    //找出成对的号码
    //output:
    //(1001250000000001,1003750000000001)
    //(1001320000000001,1003830000000001)
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
    //println("rddRawCNPairs: \n " + rddRawCNPairs.collect().mkString("\n"))          
    
    //找出孤立的号码
    //output:
    //1000960000000001
    var rddLonelyCN =   rddDirectedRawCNPairs.flatMap {
      case (((sn1, sn2), weight)) => {
        val buf = new ListBuffer[(Long, Int)]
        if (weight== 1 && sn2==0L) { //号码对
          //调试用
          buf += ((sn1, 1))
        }
        buf.toIterable
      }      
    }
    //println("rddLonelyCN: \n " + rddLonelyCN.collect().mkString("\n"))             
   
     /******** 四、找出最终的有效号码对 *******/      
    //为起点找到其编码及所有邻接点
    //output:
    //(1002700000000001,((WN8D000000,QQ25300000;QQ47900000;QQ94200000;UD4511d6d592c74237b021e17d4b28caa6),1003750000000001))
    info("this is the ddRawCNPairsJoinF place ")
    val rddRawCNPairsJoinF = HtoXGenUID.rddVidtoLinks.join(rddRawCNPairs)
    //println("rddRawCNPairsJoinF " + rddRawCNPairsJoinF.collect().mkString("\n")) 
    
    //调整一下元素位置，key放在前面
    val rddRawCNPairsWithLinksF = rddRawCNPairsJoinF.map {
      case (s_vid, ((id, links), e_vid)) => {
        (e_vid, ((s_vid,id), links))
      }
    }

    //为终点找到其编码及所有邻接点
    //output:
    //(((1002700000000001,WN8D000000),QQ25300000;QQ47900000;QQ94200000;UD4511d6d592c74237b021e17d4b28caa6),((1003750000000001,WN2B000000),QQ25400000;QQ37600000;QQ41200000;UD4511d6d592c74237b021e17d4b28caa6))
    val rddRawCNPairsJoinS = HtoXGenUID.rddVidtoLinks.join(rddRawCNPairsWithLinksF)
    val rddRawCNPairsWithLinks = rddRawCNPairsJoinS.map {
      case (e_vid, ((e_id, e_links), ((s_vid, s_id),s_links))) => {
        (((s_vid,s_id),s_links),((e_vid,e_id), e_links))
      }
    }
    //println("rddRawCNPairsWithLinks: \n " + rddRawCNPairsWithLinks.collect().mkString("\n")) 
    
    //判断是否是属于同一用户的号码对
    //建立号码之间关联  （按号码对取出子图 （）， 
    //返回: 
    //   一对号码 (是属于同一用户的两个号码)
    //   或2个虚号码对（即只有一个号码的号码对，后面用0L补上）（UIDPairGraph的判断号码对算法认为没有匹配上）
    //output:
    //((1002700000000001,WN8D000000),(0,))      没有匹配的节点
    //((1003830000000001,WNFF000000),(0,))       没有匹配的节点  
    //((1001250000000001,MN20000000),(1003830000000001,WNFF000000))         匹配的号码对 
    //((1000510000000001,MN0E000000),(1003830000000001,WNFF000000))         匹配的号码对
    val rddRealPairs = GenUIDExtPair(rddRawCNPairsWithLinks, props)
    //println("rddRealPairs: \n" + rddRealPairs.collect().mkString("\n"))     

    
    /******** 五、以通信号码为点, 以有效号码对为边, 再次构建图 *******/
    //号码之间的关联是边。
    info("this is the rddGroupEdge place ")
    var rddGroupEdge = rddRealPairs.flatMap{ case ((s_vid,s_id),(e_vid,e_id)) => {
        val buf = new ListBuffer[Edge[(String, Int)]]
        if (e_vid>0) {
          buf += Edge(s_vid, e_vid, ("PR", 1))
        }
        buf.toIterable
      }    
    }
    //println("rddGroupEdge: \n" + rddGroupEdge.collect().mkString("\n"))    
    
    //配对的点进行下一步合并归组运算
    val rddGroupVertex = rddRealPairs.flatMap{ case ((s_vid,s_id),(e_vid,e_id)) => {
        val buf = new ListBuffer[(Long, (String,  (Long, List[(Long, Long)])))]
        val neighbors : List[(Long, Long)]= List()
        if (e_vid>0) {
          buf += ((s_vid, (s_id.substring(0,2), (0L,neighbors))))
          buf += ((e_vid, (e_id.substring(0,2), (0L,neighbors)))) 
        }
          buf.toIterable
        }
      }    
     //println("rddGroupVertex: \n" + rddGroupVertex.collect().mkString("\n"))       
    
     //配对的点
     val rddPaired = rddGroupVertex.map { case (vid, (id, (0L,neighbors))) =>  (vid,1) }
      
     //未配对的点, 留到以后再用
     var rddNotPaired = rddRealPairs.flatMap{ case ((s_vid,s_id),(e_vid,e_id)) => {
     val buf = new ListBuffer[(Long, Int)]
        val neighbors : List[(Long, Long)]= List()
        if (e_vid==0L) {
          buf += ((s_vid, 1))
        }
          buf.toIterable
        }
      } .distinct().subtract(rddPaired)
     //println("rddNotPaired: \n" + rddNotPaired.collect().mkString("\n"))       

     
     val graphPairs = Graph(rddGroupVertex, rddGroupEdge, null, StorageLevel.MEMORY_AND_DISK, StorageLevel.MEMORY_AND_DISK)    
     
     /******** 六、找出图中所有通信号码所属group *******/    
    //	pregel 获得 关联的组
    info("this is the PregelGenUIDFindGroups place ")
    val graphGroup = PregelGenUIDFindGroups(graphPairs, 2, props)
    //println("graphGroup.vertices: \n" + graphGroup.vertices.collect().mkString("\n")) 
    //println("graphGroup.edges : \n" + graphGroup.edges.collect().mkString("\n")) 
    
    //需要扩展多连接的节点到多个组
    val rddVerticesExt = graphGroup.vertices.flatMap { vert =>
      {
        var verts = new ListBuffer[(Long, (String, Long))]
        val neighbors = vert._2._2._2
        if (neighbors.length == 0) {
          verts += ((vert._1, (vert._2._1, vert._2._2._1)))
        } 
        else { //需要扩展的节点,比如宽带节点
          var groupList = Set[Long]()
          for (neighbor <- neighbors) {
              if (!groupList.contains(neighbor._2))  groupList+=neighbor._2
          }            
          for (g <- groupList) 
            verts += ((vert._1, (vert._2._1, g))) 
        }
        verts.toIterable
      }
    }     
    //println("rddVerticesExt group:  \n" + rddVerticesExt.collect().mkString("\n"))      
    
    //graphx中的点还原为 邻接表节点
    val rddLinksJoinVerts = HtoXGenUID.rddVidtoLinks.join(rddVerticesExt)
    //println("rddLinksJoinVerts:  \n" + rddLinksJoinVerts.collect().mkString("\n")) 

    val rddCnndInId = rddLinksJoinVerts.map {
      case (vid, ((id, links), (typ, cvid))) => {
        (cvid, (id, links))
      }
    }    
    //找出属于同一组的节点
    var rddCnndGroup = rddCnndInId.groupByKey().map { case (v) => v._2.toList }
    //println("rddCnndGroup is:  \n " + rddCnndGroup.collect().mkString("\n"))
    
     //还需要需要加入孤立节点
     rddLonelyCN = rddLonelyCN.++(rddNotPaired)
     val rddLonelyCNwithLinks = HtoXGenUID.rddVidtoLinks.join(rddLonelyCN)

    val rddLonelyGroup = rddLonelyCNwithLinks.flatMap {
      case (vid, ((id, links), typ)) => {
        val buf = new ListBuffer[List[(String, String)]]
        if (links.length()>0) {  //异网手机号, 因为只和宽带号关联, 没有imsi被过滤掉
          buf += (List((id, links)))
        }
        buf.toIterable
      }
    }    
    //println("rddLonelyGroup is:  \n " + rddLonelyGroup.collect().mkString("\n")) 
    //println("HtoXGenUID.rddVidtoLinks is:  \n " +HtoXGenUID.rddVidtoLinks.collect().mkString("\n")) 
     
    
    
    //所有的组
    rddCnndGroup = rddCnndGroup.++(rddLonelyGroup)
    //println("New rddCnndGroup is:  \n " + rddCnndGroup.collect().mkString("\n"))

     
    /******** 七、UID生成 *******/
    //	把所有的组的信息 组成图。
    //算出优势UID，如果没有则要生成
    //Output: RDD[(String, List[(String, String)])]   即每个邻接树的所有节点保存为一个List 
    //---其中 第一个String是找到或生成的这个树的UID，  （id: 是节点id  , uid: String 记录了相邻的UID）
    //---比如   UDc7e88a94542343fa83ff7a5b6c18c57e， List((AI430851876,), (IDb5eafeb6f2e8228df4c23fc2e4f1f0b2,),...)
    info(getNowDate() + " ****** 算出优势UID，如果没有则要生成   ******")
    info("this is the GenUIDExtGroup place ")
    val rddGroupWithUID = GenUIDExtGroup(rddCnndGroup, props)
    //println("rddGroupWithUID is:  " + rddGroupWithUID.collect().mkString("\n"))
    

    /********八、计算出需要添加（更新）的UID边 *******/
    //记下所有要添加的（原来没有UID的节点，增加到），更新的（对应2条： 删除旧的[即添加权重为0的到旧]，添加新的[]）
    //Input: RDD[List[(id: String, uid: String)]]   即每个邻接树的所有节点保存为一个List. 其中 id: 是节点id  , uid: String 记录了相邻的UID
    //Output: RDD[((String, String), Long)]     ((行，列)，值）)
    //---   ((行，列)，值）)
    //---  ((IDb5eafeb6f2e8228df4c23fc2e4f1f0b2,zzUDc7e88a94542343fa83ff7a5b6c18c57e),1)
    info(getNowDate() + " ****** 计算出所有要添加的（原来没有UID的节点，增加到），更新的  ******")
    info("this is the XtoHGenUIDExt place ")
    var rddNewRelations = XtoHGenUIDExt(rddGroupWithUID)
    //println("rddNewRelations is:  " +  rddNewRelations.collect().mkString("\n"))

    /********九、保存UID边到HBase *******/
    //row  ((行，列)，值）)
    info("this is the saveToGraphTable place ")
    HBaseIO.saveToGraphTable(sc, props, rddNewRelations)
  }
}