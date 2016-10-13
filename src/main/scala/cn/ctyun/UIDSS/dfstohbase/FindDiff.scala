package cn.ctyun.UIDSS.dfstohbase

import org.apache.spark.rdd.RDD
import cn.ctyun.UIDSS.utils.{Logging }

object FindDiff extends Logging{
  def apply(rddOld: RDD[((String, String), String)], rddNew: RDD[((String, String), String)]): RDD[((String, String), String)] = {

    val rddAdd = rddNew.subtract(rddOld)
    //println("rddAdd is: \n" +  rddAdd.collect().mkString("\n"))
    
    
    val rddRemove = rddOld.subtract(rddNew).map(row => (row._1, "0")) //把需要删除的记录，权重置为 0
    //println("rddRemove is: \n" +  rddRemove.collect().mkString("\n"))

    rddAdd++rddRemove
  }
}