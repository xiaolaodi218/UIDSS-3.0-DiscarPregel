package cn.ctyun.UIDSS.cmds

import java.util.Properties
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.util.Bytes
import cn.ctyun.UIDSS.utils.{ Utils, Logging }
import cn.ctyun.UIDSS.hgraph.{ HGraphUtil, HtoXBatchQuery, GraphXUtil }
import cn.ctyun.UIDSS.hbase.HBaseIO

import cn.ctyun.UIDSS.graphxop.{ PregelBatchQuery }

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