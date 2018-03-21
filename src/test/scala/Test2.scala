import java.util.Properties

import cn.ctyun.UIDSS.uidop.GenUIDExtPair.{expandPairToGraph, findTruePair}
import org.apache.spark.graphx.Edge
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.collection.immutable._

object Test2 {
  def main(args: Array[String]): Unit = {
    val list2: List[(String, String)] = expandPairToGraph2(List(("qq123", "wn123;qq123;mn123"),("wn345", "qq345;mn345")))
    //pair:List((s_id, s_links), (e_id, e_links))
   print(list2)

  }

  def expandPairToGraph2(pair: List[(String, String)]): List[(String, String)] = {

    var g = Map[String, Set[String]]()
    for (v <- pair) {
      //把两端的节点加入图
      val fields = v._2.split(";")
      g += (v._1 -> fields.toSet)

      //把相邻节点逐个加入图
      if (fields.size > 0) {
        for (field <- fields) {
          if (g.contains(field)) {
            var neighbors = g.get(field).get
            neighbors += v._1
            g -= field
            g += (field -> neighbors)
          } else {
            var neighbors = Set[String]()
            neighbors += v._1
            g += (field -> neighbors)
          }
        }
      }
    }
    var r = ListBuffer[(String, String)]()
    for (v <- g) {
      var ns = ""
      for (n <- v._2) {
        ns = ns + ";" + n
      }
      ns = ns.substring(1)
      r += ((v._1, ns))
    }
    r.toList
  }



}
