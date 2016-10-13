package cn.ctyun.UIDSS.dfstohbase

import org.apache.spark.SparkContext
import scala.collection.mutable.ListBuffer
import org.apache.spark.rdd.RDD

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
//import com.hadoop.mapreduce.LzoTextInputFormat
//import com.hadoop.mapred.DeprecatedLzoTextInputFormat
import org.apache.hadoop.mapred.SequenceFileInputFormat

import cn.ctyun.UIDSS.hgraph.HGraphUtil

object DPIRaw {
  var weight: String = ""

  def apply(sc: SparkContext, path: String, order: String): RDD[((String, String), String)] = {
    val textFile = sc.textFile(path)
    //val textFile = sc.newAPIHadoopFile[LongWritable, Text, LzoTextInputFormat](path)
    //val textFile = sc.newAPIHadoopFile[LongWritable, Text, SequenceFileInputFormat[LongWritable,Text]](path)

    //println(textFile.collect().mkString("\n"))
    weight = order
    val result = textFile.flatMap{ convert(_)}
    //val result = textFile.flatMap{ case (_,row) => convert(row.toString())}
    //println(result.collect().mkString("\n"))
    result  }
  
  
  def convert(line: String): Iterable[((String, String), String)] = {
    val buf = ListBuffer[((String, String), String)]()

    try {

      val fields = line.split("\1")
      val PROD_INST_ID = fields(0);
      val LANT_ID = fields(1);
      val ACCS_NBR = fields(2);
      val IMSI = fields(3);
 
      if (null != ACCS_NBR && ACCS_NBR.length() > 7) {
        // 添加产品实例与手机号关系				
        buf += (((HGraphUtil.CLMN_ACCS_NUM + ACCS_NBR, HGraphUtil.CLMN_PROD_INST + LANT_ID + PROD_INST_ID), weight))

      }
    } catch {
      case e: Exception =>
    }
    buf.toIterable
  }
}