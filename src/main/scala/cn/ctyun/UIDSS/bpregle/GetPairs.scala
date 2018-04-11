package cn.ctyun.UIDSS.bpregle

import org.apache.hadoop.hbase.util.Bytes
import scala.collection.mutable.ListBuffer
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.Cell
import cn.ctyun.UIDSS.utils.Logging
import cn.ctyun.UIDSS.hgraph.HGraphUtil
import java.util.HashMap
import cn.ctyun.UIDSS.hgraph.HtoXGenUID

object GetPairs extends Logging{
  
  //==============================20180410============================
  //RDD[((Long,String),(Long,String))]
  //为了使用HtoXGenUID中的rddDstIdSrcVId，需要改动HtoXGenUID，提前声明rddDstIdSrcVId
  //最后还需要在HtoXGenUID中将生成图的方法注释掉！！！
  //为了使用rddDstIdSrcVId需要改变其格式，故修改HtoXGenUID中的maptoedge方法
  def apply(rddHBaseWithSN:RDD[(Long,Result)]):RDD[(Long,Long)] = {
    //已经经过过滤的RDD
    val filteredRDD = HtoXGenUID.connect2PairRDD.distinct()
    //获得（ID，VID）的RDD
    val IDAndVID = rddHBaseWithSN.map{
      case (sn,r) => 
        val ID = Bytes.toString(r.getRow).substring(2)
        (ID,sn)
    }
    //用号码对join IDAndVID 获得 （号码，VID）的形式再排序为了后面去重
    //(n1,n1VID).join(n1,n2).map => (n2,(n1,n1VID))
    val lefID2VID = IDAndVID.join(filteredRDD).map{
      case (n1,(n1VID,n2)) =>
        (n2,(n1,n1VID))
    }
    //[(String, (Long, (String, Long)))]
    //[(n2, (n2VID, (n1, n1VID)))] => 根据VID大小确定顺序 ((n1, n1VID),(n2, n2VID)) 或相反
    val v2 = IDAndVID.join(lefID2VID).map{
      case (n2, (n2VID, (n1, n1VID))) =>
        if(n1VID < n2VID){
          ((n1, n1VID),(n2, n2VID))
        }else{
          ((n2, n2VID),(n1, n1VID))
        }
    }
    //去重后的所有关联号码
    val distinctedNums = v2.distinct().map{
      case ((n1, n1VID),(n2, n2VID)) => 
        (n1VID,n2VID)
    }
    distinctedNums
  }
}