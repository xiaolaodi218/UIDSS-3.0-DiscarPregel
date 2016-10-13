package cn.ctyun.UIDSS.hgraph

import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._

import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes;

import scala.collection.mutable.ListBuffer

/**
 * 类描述：
 */
object XtoHGenUID {
  def getNewRelations(uid: String, group: List[(String, String)]): Iterable[((String, String), String)] = {

    val buf = new ListBuffer[((String, String), String)]
    for (vertex <- group) {
      val typ = vertex._1.substring(0, 2)
      if (typ.compareTo(HGraphUtil.STR_UD) != 0 //因为加的是双向只需要从非UID节点查起就够了
        && ( (typ.compareTo(HGraphUtil.STR_ACCS_NUM) == 0 //只需要关键节点加UID关联
          || typ.compareTo(HGraphUtil.STR_MBL_NUM) == 0  
          || typ.compareTo(HGraphUtil.STR_WB_NUM) == 0  
          || typ.compareTo(HGraphUtil.STR_CUST_ID) == 0
          || typ.compareTo(HGraphUtil.STR_ID_NUM) == 0))) {

        val strUIDTable = Bytes.toString(HGraphUtil.BYTE_TABLE_UID)
        if (vertex._2.length() > 0) { //原来有UID，需要替换

          buf += (((vertex._1, strUIDTable + vertex._2), "0"))
          buf += (((vertex._2, strUIDTable + vertex._1), "0"))

          buf += (((vertex._1, strUIDTable + uid), "1"))
          buf += (((uid, strUIDTable + vertex._1), "1"))
        } else { //原来没有UID，新加关联关系     
          buf += (((vertex._1, strUIDTable + uid), "1"))
          buf += (((uid, strUIDTable + vertex._1), "1"))
        }
      }

    }
    buf.toIterable
  }
  
  //Input: RDD[List[(id: String, uid: String)]]   即每个邻接树的所有节点保存为一个List
  //---其中   id: 是节点id  , uid: String 记录了相邻的UID
  def apply(rddGroupWithUID: RDD[(String, List[(String, String)])]): RDD[((String, String), String)] ={
    //记下所有要添加的（原来没有UID的节点，增加到），更新的（对应2条： 删除旧的[即添加权重为0的到旧]，添加新的[]）
    rddGroupWithUID.flatMap { case (uid, group) => getNewRelations(uid, group) }
  }
}