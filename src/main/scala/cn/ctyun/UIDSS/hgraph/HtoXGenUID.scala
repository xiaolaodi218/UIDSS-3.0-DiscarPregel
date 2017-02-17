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
package cn.ctyun.UIDSS.hgraph

import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._

import org.apache.spark.storage.StorageLevel

import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes;

import scala.collection.mutable.ListBuffer

import cn.ctyun.UIDSS.utils.{ Logging }

import org.apache.spark.Accumulator
import org.apache.spark.SparkContext

/**
 * 类描述：生成UID操作
 * 1. 生成图时, 需要把关联的UID写入节点的属性
 * 2. 遍历生成关联树后,找出优势UID
 * 3. 更新所有关联节点
 *
 * 需要改节点结构，String用来记录UID
 * 一次广播先为所有节点找到相邻UID。 需要广播UID（初始步骤）， 接收，保存
 * 下次广播找到相邻树
 * 生成包含每个节点的UID信息的相邻树RDD
 * 算出优势UID，
 * 记下所有要添加的（原来没有UID的节点，增加到），更新的（对应2条： 删除旧的[即添加权重为0的到旧]，添加新的[]）
 * 保存到数据库
 */
object HtoXGenUID extends Logging {
  private var printCount = 1
  def getPrintCount() = {
    if (printCount > 0) {
      printCount = printCount - 1
      1
    } else {
      0
    }
  }
  
  private var printCount_1 = 1
  def getPrintCount_1() = {
    if (printCount_1 > 0) {
      printCount_1 = printCount_1 - 1
      1
    } else {
      0
    }
  }
  
  var rddVidtoLinks: RDD[(Long, (String, String))] = null

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

  def isVertNeedProcessing(row: String, iNeighbors: Int): Boolean = {
    var bNeedProcess = true
    row.substring(0, 2) match {
      case HGraphUtil.STR_CUST_ID  => { if (iNeighbors > 10) bNeedProcess = false }
      case HGraphUtil.STR_ID_NUM   => { if (iNeighbors > 10) bNeedProcess = false }
      case HGraphUtil.STR_QQ       => { if (iNeighbors > 10) { bNeedProcess = false } }
      //手机号关联比较多    9(+有多个ME) + 11(9 与IT侧有2重)  + UID
      case HGraphUtil.STR_MBL_NUM  => { if (iNeighbors > 30) bNeedProcess = false }
      //宽带号关联比较多  5 + 33 + 多个UID(网络,CI, ID关联的)
      case HGraphUtil.STR_WB_NUM   => { if (iNeighbors > 50) bNeedProcess = false }
      // 5  + 多个UID(CI, ID关联的) 
      case HGraphUtil.STR_ACCS_NUM => { if (iNeighbors > 20) bNeedProcess = false }
      // UID 最多拥有200个号        
      case HGraphUtil.STR_UD       => { if (iNeighbors > 200) bNeedProcess = false }
      case _    => { bNeedProcess = false }
    }
    bNeedProcess
  }

  //保存到邻接列表以后用
  //把HBase一行, 就是一个点的所有邻居   ( 1000001234567 , "AN12345,MN678910,...")
  def convertToLinks(sn: Long, v: Result): (Long, (String, String)) = {
    var links = ""
    val row = Bytes.toString(v.getRow)

    //超大节点暂不考虑
    val iNeighbors = v.rawCells().size
    if (true) {
      for (c <- v.rawCells()) {
        var dst = Bytes.toString(c.getQualifier)
        dst = dst.substring(2)
        var value = 0
        try {
          value = Bytes.toInt(c.getValue)
        } catch {
          case _: Throwable =>
        }
        if (value > 0)
          if (links.length() > 0) {
            links = links + ";" + dst
          } else {
            links = dst
          }
      }
    }
    (sn, (row, links))
  }

  //把HBase一行转换成多条边，目标节点id, (源节点sn, 连接类型)）
  //可以在这一步进行边的过滤
  def mapToEdges(sn: Long, v: Result): Iterable[(String, (Long, String))] = {
    var needPrint = false
    val buf = new ListBuffer[(String, (Long, String))]

    //超大节点暂不考虑
    val row = Bytes.toString(v.getRow)
    val iNeighbors = v.rawCells().size
    if (isVertNeedProcessing(row, iNeighbors)) {
      //if (isDebugVert(row)) {  needPrint = true }
      for (c <- v.rawCells()) {
        var dst = Bytes.toString(c.getQualifier)
        val ty = dst.substring(0, 2)
        dst = dst.substring(2)
        //if (isDebugVert(dst)) { needPrint = true }
        var value = 0
        try {
          value = Bytes.toInt(c.getValue)
        } catch {
          case _: Throwable =>
        }
        if (value > 0) {
          if (needPrint) {
           buf += ((dst, (sn, ty+row+";"+dst))) 
          }else {
           buf += ((dst, (sn, ty))) 
          }          
        }
      }
    }

    if (needPrint) {
      println("**********row  is " + row)
      println("**********neighbor list  " + buf.mkString("|") + " is added to graph")
    }

    buf.toIterable
  }

  //Graph[点 (String 点类型, Long 根节点序号),  边(String 边类型, Int 权重)] 
  //def getGraphRDD(rdd: RDD[(ImmutableBytesWritable, Result)], sc: SparkContext): Graph[(String, Long), (String, Int)] = {
  def getGraphRDD(rddHBaseWithSN: RDD[(Long, Result)], sc: SparkContext): Graph[(String, Long), (String, Int)] = {
    //-------------------------------------------------
    //点序号
    //-------------------------------------------------
//    //生成序号
//    //为每个起始点加序列号sn，也就是HBase分区序号*每分区最大行数 + 该分区内的行序号。
//    //比如： （100100001, {AI430851876/v:$1AN06F6642CE07804C26B847BEAEEB0204A/1458733523410/Put/vlen=4/mvcc=0} ) 
//    info(" ****** 点序号  ******")
//    val rddHBaseWithSN = rdd
//      .mapPartitionsWithIndex { (ind, vs) =>
//        var sn = GraphXUtil.MAX_VERTICE + ind * GraphXUtil.MAX_VERTICE_PER_PARTITION
//        val lst = vs.map {
//          case (_, v) =>
//            sn = sn + 1
//            val row = Bytes.toString(v.getRow)
//            if (isDebugVert(row)) {  println(row + "  is assigned sn of " + sn + " in partition " + ind) }
//            (sn, v)
//        }
//        println("Partition  " + ind + " has  total sn of " + sn)
//        lst
//      }

   //println("rddHBaseWithSN  has  " + rddHBaseWithSN.count() + " rows") 
    
    info(" ****** 点序号: 生成序号   ******")
    //点id到点序号对应关系
    //（点id，(点序号,"sn")） 
    // 比如： （AI430851876，（100100001,"AI"））
    //val rddIdtoVId = rddHBaseWithSN.map[(String, (Long, String))] {
    val rddIdtoVId = rddHBaseWithSN.map {
      case (sn, v) =>
        val id = Bytes.toString(v.getRow)
        if (HtoXGenUID.getPrintCount() > 0) {
          println("**************************id is " + id +"\n")
          println("**************************sn is " + sn +"\n")
        }
        //if (isDebugVert(id)) {  println(id + "  is assigned sn of " + sn) }
        (id, (sn, id.substring(0, 2)))
    }

    info(" ****** 点序号: 点id到点序号对应关系   ******")
    //-------------------------------------------------
    //点集合
    //-------------------------------------------------

    info(" ****** 点集合: 第1步   ******")
    //点集合第1步
    //Vertex (vid: Long, (id: String, temp:Long ))
    //vid: Long 点序号, (id: String 点类型, temp:Long 根节点序号 )
    info(" ****** 点集合   ******")
    val rddVertex = rddIdtoVId.map {
      case (id, (sn, _)) => {
        //if (isDebugVert(id)) {
        if (false) {
          (sn, (id, 0L))
        } else {
          (sn, (id.substring(0, 2), 0L))
        }
      }
    }
    //点集合过滤, 暂时不做. 
    //    val rddVertex = rddHBaseWithSN.flatMap[(Long, (String, Long))]({
    //      case (sn, v) => {
    //        val buf = new ListBuffer[(Long, (String, Long))]
    //        //超大节点暂不考虑
    //        val row = Bytes.toString(v.getRow)
    //        val iNeighbors = v.rawCells().size
    //        if (isVertNeedProcessing(row, iNeighbors)) {
    //          buf += ((sn, (row.substring(0, 2), 0L)))
    //        }
    //        buf.toIterable
    //      }
    //    })
    //println("rddVertex " + rddVertex.collect().mkString("\n"))

    //为以后计算邻接点, 先保留下
    rddVidtoLinks = rddHBaseWithSN.map[(Long, (String, String))] {
      case (sn, v) => convertToLinks(sn, v)
    }.persist(StorageLevel.MEMORY_AND_DISK_SER)
    //println("rddSnAndLinks " + rddSnAndLinks.collect().mkString("\n"))

    //-------------------------------------------------
    //边集合  
    //-------------------------------------------------
    info(" ****** 边集合   ******")

    info(" ****** 边集合:  第1步 ******")
    //边集合第1步 - 点到点关系，同时替换起点序号
    //(终止点id, (起始点序号，边属性）)
    //比如：(AI430851876, (100100001，"$1"))
    //val rddDstIdSrcVId = rddHBaseWithSN.flatMap[(String, (Long, String))] {
    val rddDstIdSrcVId = rddHBaseWithSN.flatMap {  
      case (sn, v) =>
        if (HtoXGenUID.getPrintCount_1() > 0) {
          println("**************************sn is " + sn +"\n")
          println("**************************v is " + v +"\n")
        }
        mapToEdges(sn, v)
    }
    //rddVtoV.foreach {
    //  case (dst, (sn, typ)) =>
    //    println("Dst node id is: " + dst + ";  source node serial number is: " + sn + "; link type is:  " + typ)
    //}

    info(" ****** 边集合:  第2步 ******")
    //边集合第2步 - 通过join实现终点id与序号对应
    //Input:
    //(dstId, (srcVId, edgeTyp))
    //(id, (vid, typ))
    //Output:
    //(dstId, ((srcVId, edgeTyp), (vid, typ)))
    val rddJoinByDstId = rddDstIdSrcVId.join(rddIdtoVId)
    //println(rddVtoVwithSN.collect().mkString("\n"))

    info(" ****** 边集合:  第3步 ******")
    //边集合第3步 - 替换终点序号，生成边的triplet
    //Input:
    //   (dstId, ((srcVId, edgeTyp), (dstVid, nodeTyp)))
    //   (id, ((sn, typ), (snsn, sntyp)))
    //Output:
    //   Edge ((src: VertexId, dst: VertexId),  (typ: String , weight: Int) )
    val rddDirectedEdge = rddJoinByDstId.map {
      case (dstId, ((srcVId, edgeTyp), (dstVid, nodeTyp))) => {
        if (edgeTyp.length()>2) {println("Directed Edge:  (" + srcVId +" , "+ dstVid + "); Edgetype is  "+ edgeTyp +"; Dst Id is"+dstId)}
        if (srcVId > dstVid) {
          ((dstVid, srcVId), (edgeTyp, 1))
        } else {
          ((srcVId, dstVid), (edgeTyp, 1))
        }
      }
    }
    //println("rddDirectedEdge " + rddDirectedEdge.collect().mkString("\n"))

    val rddUnDirectedEdge = rddDirectedEdge.reduceByKey {
      case ((typ1, weight1), (typ2, weight2)) => {
        if (typ1.length()>2 || typ2.length()>2) {println("Before reduce Type 1 is " + typ1 +", weight 1 is "+ weight1 +", type 2 is " + typ2 +", weight 2 is "+ weight2 )}
        (typ1, weight1 + weight2) //为了过滤,双向都有才保留
      }
    }
    //println("rddUnDirectedEdge " + rddUnDirectedEdge.collect().mkString("\n"))
    
    val rddEdge = rddUnDirectedEdge.flatMap {
      case (((sn1, sn2), (typ, weight))) => {
        val buf = new ListBuffer[Edge[(String, Int)]]
        //为了过滤,双向都有才保留
        if (weight > 1) {
          if (typ.length()>2) {println("Undirected edge (" + sn1 +"," + sn2 + "," + typ+ "," + weight + ")")}          
          buf += Edge(sn1, sn2, (typ, 1))
        }
        buf.toIterable
      }
    }    
    //println("rddEdge " + rddEdge.collect().mkString("\n"))
 
    
    //-------------------------------------------------
    //生成图
    //-------------------------------------------------
    Graph(rddVertex, rddEdge, null)
  }
}