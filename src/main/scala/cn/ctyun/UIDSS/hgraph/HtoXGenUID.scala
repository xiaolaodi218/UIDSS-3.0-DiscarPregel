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
package cn.ctyun.UIDSS.hgraph

import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._

import org.apache.spark.storage.StorageLevel

import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.Cell;

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
//================================20180410===============================
  var connect2PairRDD:RDD[(String,String)] = null
  var UPTN:RDD[Long] = null
//================================20180410===============================
  
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

  def isVertNeedProcessing(row: String, cells: Array[Cell]): Boolean = {
    var bNeedProcess = true
    val iNeighbors = cells.size
    row.substring(0, 2) match {
      //不会构成关联条件的ID, CI节点被过滤掉      
      case HGraphUtil.STR_CUST_ID => { 
          var iMN=0
          var iWN=0
          for (cell<-cells ) {
            //取出每列中的值
            val cellType = Bytes.toString(cell.getQualifier).substring(2, 2)  
            if (HGraphUtil.STR_MBL_NUM.compareTo(cellType)==0) iMN+=1
            if (HGraphUtil.STR_MBL_NUM.compareTo(cellType)==0) iWN+=1
          }
          if (iNeighbors > 10 || iMN>1 || iWN > 1) bNeedProcess = false 
        }
       //不会构成关联条件的ID, CI节点被过滤掉          
      case HGraphUtil.STR_ID_NUM   => { 
          var iMN=0
          var iWN=0
          for (cell<-cells ) {
            val cellType = Bytes.toString(cell.getQualifier).substring(2, 2)  
            if (HGraphUtil.STR_MBL_NUM.compareTo(cellType)==0) iMN+=1
            if (HGraphUtil.STR_MBL_NUM.compareTo(cellType)==0) iWN+=1
          }
          if (iNeighbors > 10 || iMN>1 || iWN > 1) bNeedProcess = false 
      }
      case HGraphUtil.STR_QQ       => { if (iNeighbors > 10) { bNeedProcess = false } }
      case HGraphUtil.STR_WE_CHAT       => { if (iNeighbors > 10) { bNeedProcess = false } }
      //手机号关联比较多    9(+有多个ME) + 11(9 与IT侧有2重)  + UID
      case HGraphUtil.STR_MBL_NUM  => { if (iNeighbors > 30) bNeedProcess = false }
      //宽带号关联比较多  5 + 33 + 多个UID(网络,CI, ID关联的)
      case HGraphUtil.STR_WB_NUM   => { if (iNeighbors > 50) bNeedProcess = false }
      // 5  + 多个UID(CI, ID关联的) 
      case HGraphUtil.STR_ACCS_NUM => { if (iNeighbors > 20) bNeedProcess = false }
      // UID 最多拥有200个号        
      case HGraphUtil.STR_UD       => { if (iNeighbors > 200) bNeedProcess = false }
      case HGraphUtil.STR_IMSI        => { if (iNeighbors > 10) bNeedProcess = false }
      case _                       => { bNeedProcess = false }
    }
    bNeedProcess
  }

  def isEdgeNeedProcessing(row: String, dstType: String): Boolean = {
    var bNeedProcess = false
    row.substring(0, 2) match {
      case HGraphUtil.STR_CUST_ID => {
        dstType match {
          case HGraphUtil.STR_ACCS_NUM => bNeedProcess = true
          case HGraphUtil.STR_MBL_NUM  => bNeedProcess = true
          case HGraphUtil.STR_WB_NUM   => bNeedProcess = true
          case _                       => bNeedProcess = false
        }
      }
      case HGraphUtil.STR_ID_NUM => {
        dstType match {
          case HGraphUtil.STR_ACCS_NUM => bNeedProcess = true
          case HGraphUtil.STR_MBL_NUM  => bNeedProcess = true
          case HGraphUtil.STR_WB_NUM   => bNeedProcess = true
          case _                       => bNeedProcess = false
        }
      }
      case HGraphUtil.STR_QQ => {
        dstType match {
          case HGraphUtil.STR_MBL_NUM => bNeedProcess = true
          case HGraphUtil.STR_WB_NUM  => bNeedProcess = true
          case HGraphUtil.STR_UD      => bNeedProcess = true
          case _                      => bNeedProcess = false
        }
      }
       case HGraphUtil.STR_WE_CHAT=> {
        dstType match {
          case HGraphUtil.STR_MBL_NUM => bNeedProcess = true
          case HGraphUtil.STR_WB_NUM  => bNeedProcess = true
          case HGraphUtil.STR_UD      => bNeedProcess = true
          case _                      => bNeedProcess = false
        }
      }
      //手机号关联
      case HGraphUtil.STR_MBL_NUM => {
        dstType match {
          case HGraphUtil.STR_QQ      => bNeedProcess = true
          case HGraphUtil.STR_WE_CHAT      => bNeedProcess = true          
          case HGraphUtil.STR_CUST_ID => bNeedProcess = true
          case HGraphUtil.STR_ID_NUM  => bNeedProcess = true
          case HGraphUtil.STR_IMSI    => bNeedProcess = true
          case HGraphUtil.STR_UD      => bNeedProcess = true
          case _                      => bNeedProcess = false
        }
      }
      //宽带号关联
      case HGraphUtil.STR_WB_NUM => {
        dstType match {
          case HGraphUtil.STR_QQ      => bNeedProcess = true
          case HGraphUtil.STR_WE_CHAT      => bNeedProcess = true          
          case HGraphUtil.STR_CUST_ID => bNeedProcess = true
          case HGraphUtil.STR_ID_NUM  => bNeedProcess = true
          case HGraphUtil.STR_UD      => bNeedProcess = true
          case _                      => bNeedProcess = false
        }
      }
      //固话号 
      case HGraphUtil.STR_ACCS_NUM => {
        dstType match {
          case HGraphUtil.STR_CUST_ID => bNeedProcess = true
          case HGraphUtil.STR_ID_NUM  => bNeedProcess = true
          case HGraphUtil.STR_UD      => bNeedProcess = true
          case _                      => bNeedProcess = false
        }
      }
      // UID        
      case HGraphUtil.STR_UD => {
        dstType match {
          case HGraphUtil.STR_ACCS_NUM => bNeedProcess = true
          case HGraphUtil.STR_WB_NUM   => bNeedProcess = true
          case HGraphUtil.STR_MBL_NUM  => bNeedProcess = true
          case HGraphUtil.STR_QQ       => bNeedProcess = true
          case HGraphUtil.STR_WE_CHAT       => bNeedProcess = true
          case _                       => bNeedProcess = false
        }
      }
      //sim卡
      case HGraphUtil.STR_IMSI => {
        dstType match {
          case HGraphUtil.STR_MBL_NUM => bNeedProcess = true
          case _                      => bNeedProcess = false
        }
      }
      case _ => { bNeedProcess = false }
    }
    bNeedProcess
  }

  //保存到邻接列表以后用
  //把HBase一行, 就是一个点的所有邻居   ( 1000001234567 ,(AN12345,MN678910:WN2345...))
  def convertToLinks(sn: Long, v: Result): (Long, (String, String)) = {
    var links = ""
    val row = Bytes.toString(v.getRow.drop(2))

    //超大节点暂不考虑
    if (isVertNeedProcessing(row, v.rawCells())) {
      for (c <- v.rawCells()) {
        var dst = Bytes.toString(c.getQualifier)
        dst = dst.substring(2)
        val nodeTy = dst.substring(0, 2)
        //只计算与UID生成有关节点
        if (isEdgeNeedProcessing(row, nodeTy)) {
          
          var value = 0
          try {
            value = Bytes.toInt(c.getValue)
          } catch {
            case _: Throwable =>
          }
          if (value > 0) {
            if (links.length() > 0) {
              links = links + ";" + dst
            } else {
              links = dst
            }
          }
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
    val row = Bytes.toString(v.getRow.drop(2))
    if (isVertNeedProcessing(row,  v.rawCells())) {
      //if (isDebugVert(row)) {  needPrint = true }
      for (c <- v.rawCells()) {
        var dst = Bytes.toString(c.getQualifier)
        val ty = dst.substring(0, 2)
        dst = dst.substring(2)
        val nodeTy = dst.substring(0, 2)
        if (isEdgeNeedProcessing(row, nodeTy)) {
          //if (isDebugVert(dst)) { needPrint = true }
          var value = 0
          try {
            value = Bytes.toInt(c.getValue)
          } catch {
            case _: Throwable =>
          }
          if (value > 0) {
            if (needPrint) {
              buf += ((dst, (sn, ty + row + ";" + dst)))
            } else {
              buf += ((dst, (sn, ty)))
            }
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
 
//==============================20180410============================
  def map2ConnectRDD(sn: Long, v: Result): Iterable[(String, String)] = {
    var needPrint = false
    //将号码对保存在pairs中
    val pairs = new ListBuffer[(String,String)]()

    val row = Bytes.toString(v.getRow.drop(2))
    //超大节点暂不考虑
    if (isVertNeedProcessing(row,  v.rawCells())) {
      //只保留连接节点
      val rowType = Bytes.toString(v.getRow).substring(2, 4)
      var process = false
      rowType match {
        case "ID" => process = true
        case "CI" => process = true
        case "QQ" => process = true
        case "WC" => process = true
        case "UD" => process = true
        case _ => process = false
      }
      //将连接节点的邻居节点展开为号码对，如：ID邻居有 MN123,AN123,WN123,MN456
      //转换成（MN123,AN123），（MN123，WN123），（MN123，MN456）以此类推
      //将所有号码存入nums中
      val nums = new ListBuffer[String]()
      if(process){
          for (c <- v.rawCells()) {
          var dst = Bytes.toString(c.getQualifier)
          val ty = dst.substring(0, 2)
          dst = dst.substring(2)
          val nodeTy = dst.substring(0, 2)
          if (isEdgeNeedProcessing(row, nodeTy)) {
            if(nodeTy.equals("AN")||nodeTy.equals("MN")||nodeTy.equals("WN")){             
              	var value = 0
          			try {
          				value = Bytes.toInt(c.getValue)
          			} catch {
          			  case _: Throwable =>
          			}
              	if (value > 0) {
              		nums += dst            		
              	}            	
            }
          }
        }
      }
      //循环将所有号码组成号码对
      var loop = 1    
      for(num <- nums){
        var inloop = loop
        while(inloop < nums.length){
          pairs += ((num, nums(inloop)))
          inloop += 1
        }
        loop += 1
      }
      
    }
    pairs.toIterable
  }
 
//==============================20180410============================
  
  //Graph[点 (String 点类型, Long 根节点序号),  边(String 边类型, Int 权重)] 
  //def getGraphRDD(rdd: RDD[(ImmutableBytesWritable, Result)], sc: SparkContext): Graph[(String, Long), (String, Int)] = {
  def getGraphRDD(rddHBaseWithSN: RDD[(Long, Result)], sc: SparkContext): Graph[(String, (Long, List[(Long, Long)])), (String, Int)] = {
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
    //            val row = Bytes.toString(v.getRow.drop(2))
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
        val id = Bytes.toString(v.getRow.drop(2))
        if (HtoXGenUID.getPrintCount() > 0) {
          println("**************************id is " + id + "\n")
          println("**************************sn is " + sn + "\n")
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
        val neighbors : List[(Long, Long)]= List()
        //if (isDebugVert(id)) {
        if (false) {
          (sn, (id, (0L, neighbors)))
        } else {
          (sn, (id.substring(0, 2), (0L,neighbors)))
        }
      }
    }
    //点集合过滤, 暂时不做. 
    //    val rddVertex = rddHBaseWithSN.flatMap[(Long, (String, Long))]({
    //      case (sn, v) => {
    //        val buf = new ListBuffer[(Long, (String, Long))]
    //        //超大节点暂不考虑
    //        val row = Bytes.toString(v.getRow.drop(2))
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
    }
//    val sampledWordCounts = rddVidtoLinks.countByKey()
//    sampledWordCounts.foreach {
//      case (sn, v) =>
//        if (v > 100) {
//          println("***rddVidtoLinks large key is " + sn + "; v is " + v + "\n")
//        }
//    }
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
          println("**************************sn is " + sn + "\n")
          println("**************************v is " + v + "\n")
        }
        mapToEdges(sn, v)
    }
//==========================20180410=====================================    
    connect2PairRDD = rddHBaseWithSN.flatMap {
      case (sn, v) =>
       map2ConnectRDD(sn,v)
    }
    
    UPTN = rddHBaseWithSN.flatMap {
      case (sn, v) =>
         val buf = new ListBuffer[Long]
         val row = Bytes.toString(v.getRow.drop(2))
         if (isVertNeedProcessing(row,  v.rawCells())) { 
         val rowType = row.substring(0, 2)
         var keep = false
         rowType match {
            case "AN" => keep = true
            case "WN" => keep = true
            case "MN" => keep = true
            case _ => keep = false
         }
         if(keep){
            buf += sn
         }
      }
      buf.toIterable
    }
//==========================20180410=====================================  
    //rddVtoV.foreach {
    //  case (dst, (sn, typ)) =>
    //    println("Dst node id is: " + dst + ";  source node serial number is: " + sn + "; link type is:  " + typ)
    //}

//    val sampledWordCounts = rddDstIdSrcVId.countByKey()
//    sampledWordCounts.foreach {
//      case (sn, v) =>
//        if (v > 100) {
//          println("**************************sn is " + sn + " v is " + v + "\n")
//        }
//    }

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
        //为什么edgtype会大于2？
        if (edgeTyp.length() > 2) { println("Directed Edge:  (" + srcVId + " , " + dstVid + "); Edgetype is  " + edgeTyp + "; Dst Id is" + dstId) }
        //组成一个小序号在前，大序号在后的一个边
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
        if (typ1.length() > 2 || typ2.length() > 2) { println("Before reduce Type 1 is " + typ1 + ", weight 1 is " + weight1 + ", type 2 is " + typ2 + ", weight 2 is " + weight2) }
        (typ1, weight1 + weight2) //为了过滤,双向都有才保留
      }
    }
    //println("rddUnDirectedEdge " + rddUnDirectedEdge.collect().mkString("\n"))

    val rddEdge = rddUnDirectedEdge.flatMap {
      case (((sn1, sn2), (typ, weight))) => {
        val buf = new ListBuffer[Edge[(String, Int)]]
        //为了过滤,双向都有才保留
        if (weight > 1) {
          //调试用
          if (typ.length() > 2) { println("Undirected edge (" + sn1 + "," + sn2 + "," + typ + "," + weight + ")") }          
          buf += Edge(sn1, sn2, (typ, 1))
        }
        buf.toIterable
      }
    }
    //println("rddEdge " + rddEdge.collect().mkString("\n"))

    //-------------------------------------------------
    //生成图
    Graph(rddVertex, rddEdge, null, StorageLevel.MEMORY_AND_DISK, StorageLevel.MEMORY_AND_DISK)
  }
}