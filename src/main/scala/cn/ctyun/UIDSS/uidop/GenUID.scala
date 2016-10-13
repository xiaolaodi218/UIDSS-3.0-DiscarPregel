package cn.ctyun.UIDSS.uidop

import java.util.Properties
import java.util.UUID
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.util.Bytes
import scala.collection.mutable.ListBuffer
import cn.ctyun.UIDSS.hgraph.{ HGraphUtil, GraphXUtil }

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
object GenUID {

  val initialMsg = GraphXUtil.MAX_VERTICE
  var UID_PRIOR_QQ = 100
  var UID_PRIOR_ID = 5
  var UID_PRIOR_CI = 5
  var UID_PRIOR_AN = 1

  def generateUID(): String = {
    UUID.randomUUID().toString().replace("-", "")
  }

  def getUID(group: List[(String, String)]): (String, List[(String, String)]) = {
    var uidCounts = Map[String, Int]()
    for (vertex <- group) {
      var count = 0
      var iCount = uidCounts.get(vertex._2)
      if (iCount.isDefined) {
        count = iCount.get
      } else {
        count = 0
      }

      val typ = vertex._1.substring(0, 2)
      typ match {
        case HGraphUtil.STR_QQ       => count + UID_PRIOR_QQ
        case HGraphUtil.STR_CUST_ID  => count + UID_PRIOR_CI
        case HGraphUtil.STR_ID_NUM   => count + UID_PRIOR_ID
        case HGraphUtil.STR_ACCS_NUM => count + UID_PRIOR_AN
        case HGraphUtil.STR_MBL_NUM => count + UID_PRIOR_AN
        case HGraphUtil.STR_WB_NUM => count + UID_PRIOR_AN       
        case _                       => count = count + 1
      }
      uidCounts += (vertex._2 -> count)
    }

    //出现次数最多的UID被选为有效UID
    var maxcount = 0
    var maxUid = ""
    uidCounts.foreach {
      case (rowkey, cnt) => {
        if (cnt > maxcount) {
          maxcount = cnt
          maxUid = rowkey
        }
      }
    }

    if (maxUid.length() == 0) maxUid = HGraphUtil.STR_UD + generateUID()

    (maxUid, group)
  }

  def apply(rddCnndGroup: RDD[List[(String, String)]], props: Properties) = {

    if (props.getProperty("UID_PRIOR_QQ").length() > 0) UID_PRIOR_QQ = props.getProperty("UID_PRIOR_QQ").toInt
    if (props.getProperty("UID_PRIOR_ID").length() > 0) UID_PRIOR_ID = props.getProperty("UID_PRIOR_ID").toInt
    if (props.getProperty("UID_PRIOR_CI").length() > 0) UID_PRIOR_CI = props.getProperty("UID_PRIOR_CI").toInt
    if (props.getProperty("UID_PRIOR_AN").length() > 0) UID_PRIOR_AN = props.getProperty("UID_PRIOR_AN").toInt

    //UID生成
    //算出优势UID，如果没有则要生成
    rddCnndGroup.map { case (group) => getUID(group) }

  }
}