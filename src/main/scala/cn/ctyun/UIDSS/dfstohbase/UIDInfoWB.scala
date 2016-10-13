package cn.ctyun.UIDSS.dfstohbase

import org.apache.spark.SparkContext
import scala.collection.mutable.ListBuffer
import org.apache.spark.rdd.RDD

import cn.ctyun.UIDSS.hgraph.HGraphUtil
import cn.ctyun.UIDSS.utils.{Logging }

object UIDInfoWB extends Logging{
  var weight: String = "1"
  var count =0
  
  def apply(sc: SparkContext, path: String, order: String, lant_id: String): RDD[((String, String), String)] = {
    val textFile = sc.textFile(path)
    weight = order
    val result = textFile.flatMap(convert(_,lant_id))
    //println(result.collect().mkString("\n"))
    info("UIDSS: UIDInfoWB processed " + count + " lines.")
    result
  }
    
  /*UID_INFO_WB
   *   账期	MONTH_ID	STRING   一级分区
   *   归属省	PROV_ID	STRING   二级分区
   *0   归属城市	LANT_ID	STRING
   *1   用户ID	PROD_INST_ID	STRING
   *2   用户号码	ACCS_NBR	STRING
   *3   身份证号	ID_NUM	STRING
   *4   客户ID	CUST_ID	STRING
   *5   客户名称	CUST_NAME	STRING
   *6   账户ID	ACCT_ID	STRING
   */
  
  def convert(line: String, lant: String): Iterable[((String, String), String)] = {
    val buf = ListBuffer[((String, String), String)]()

    try {
      val fields = line.split("\1",-1)
      
      var base = 0
      var LANT_ID = ""  
      if (lant.length()>0) {
        base = -1
        LANT_ID = lant  
      } else {
        LANT_ID = fields(base)    
      }        
      
      //val PROVIN_ID = LANT_ID.substring(0, 3)      
      val PROD_INST_ID = fields(base+1);
      val WB_NBR = fields(base+2); 
      val ID_NUM = fields(base+3);
      val CUST_ID = fields(base+4);
      val ACCT_ID = fields(base+6);

      if (null != WB_NBR && WB_NBR.length() > 7) {
        // 添加产品实例与手机号关系				
        if (null != PROD_INST_ID && PROD_INST_ID.length() > 5) {
          buf += (((HGraphUtil.STR_WB_NUM + WB_NBR, HGraphUtil.STR_TABLE_UID_INFO_WB + HGraphUtil.STR_PROD_INST + LANT_ID + PROD_INST_ID), weight))
          buf += (((HGraphUtil.STR_PROD_INST + LANT_ID + PROD_INST_ID, HGraphUtil.STR_TABLE_UID_INFO_WB + HGraphUtil.STR_WB_NUM + WB_NBR), weight))
        }
        
        // 添加手机号与身份证号关系
        if (null != ID_NUM && ID_NUM.length() > 5) { 
          buf += (((HGraphUtil.STR_WB_NUM + WB_NBR,  HGraphUtil.STR_TABLE_UID_INFO_WB + HGraphUtil.STR_ID_NUM + ID_NUM), weight))
          buf += (((HGraphUtil.STR_ID_NUM + ID_NUM,  HGraphUtil.STR_TABLE_UID_INFO_WB + HGraphUtil.STR_WB_NUM + WB_NBR), weight))
        }  

        // 添加手机号与客户ID关系
        if (null != CUST_ID && CUST_ID.length() > 5) { 
          buf += (((HGraphUtil.STR_WB_NUM + WB_NBR,  HGraphUtil.STR_TABLE_UID_INFO_WB + HGraphUtil.STR_CUST_ID + LANT_ID + CUST_ID), weight))
          buf += (((HGraphUtil.STR_CUST_ID + LANT_ID + CUST_ID,  HGraphUtil.STR_TABLE_UID_INFO_WB + HGraphUtil.STR_WB_NUM + WB_NBR), weight))
        }  		

        // 添加ACCT_ID与手机号关系
        if (null != ACCT_ID && ACCT_ID.length() > 5 ) { 
          buf += (((HGraphUtil.STR_WB_NUM + WB_NBR,  HGraphUtil.STR_TABLE_UID_INFO_WB + HGraphUtil.STR_ACCT_ID + ACCT_ID), weight))
          buf += (((HGraphUtil.STR_ACCT_ID + ACCT_ID,  HGraphUtil.STR_TABLE_UID_INFO_WB + HGraphUtil.STR_WB_NUM + WB_NBR), weight))
        }
        count += 1
      }
      
    } catch {
      case e: Exception =>
    }
    buf.toIterable
  }
}