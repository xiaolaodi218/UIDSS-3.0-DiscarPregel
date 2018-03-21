package cn.ctyun.UUIDS.realpair

import org.apache.hadoop.hbase.util.Bytes
import scala.collection.mutable.ListBuffer
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.Cell
import cn.ctyun.UIDSS.utils.Logging
import cn.ctyun.UIDSS.hgraph.HGraphUtil
import java.util.HashMap

object GenerateRealPairs extends Logging {
  def realPairs(rddHBaseWithSN:RDD[(Long,Result)]):RDD[((Long,String),(Long,String))] = {    
    //过滤无用节点
    val keepNodeRDD = rddHBaseWithSN.filter{
      (v) =>
        var keep = false
        val ty = Bytes.toString(v._2.getRow).substring(2,4)
        ty match {
          case "ID" => keep = true
          case "CI" => keep = true
          case "QQ" => keep = true
          case "WC" => keep = true
          case "AN" => keep = true
          case "MN" => keep = true
          case "WN" => keep = true
          case "UD" => keep = true
          case "IS" => keep = true
          case _    => 
        }
        keep
     }.persist(StorageLevel.MEMORY_AND_DISK)
    //output: (sn,(rowKey,links)) links经过部分过滤
    val connectRDD = keepNodeRDD.map{
      (v) =>
        val row = Bytes.toString(v._2.getRow).substring(2)
        val ty = Bytes.toString(v._2.getRow).substring(2,4)
        if(ty.equals("AN")||ty.equals("WM")||ty.equals("MN")){
          (0L,("",""))
        }else{
        	val qualifiers = v._2.rawCells()
    			var value = ""
    			var MN = 0
    			var orther = 0
    			for(q <- qualifiers){
    				val ty = Bytes.toString(q.getQualifier).substring(2,4)
						val qualifier = Bytes.toString(q.getQualifier).substring(2)
						ty match {
						case "WN" => value += ";"+qualifier
						case "MN" => {value += ";"+qualifier; MN += 1}
						case "AN" => value += ";"+qualifier
						case _    => orther += 1 //调试用，记录非通信节点数量
    				}
    			}
        	if(v._2.size()>1000){
        		info("===="+row+"==has=="+orther+"==orther nodes==")        	  
        	}
        	(v._1,(ty+MN,value.substring(1)))                   
        }        
    }.filter((v) => v._1>0)
    
    val pairRDD = connectRDD.flatMap{     
      (r) =>
        var pairs = new ListBuffer[((String,String),String)]()
        val qualifiers = r._2._2.split(";")
        //可以在此增加过滤超大节点
        var loop = 0
        for(q <- qualifiers){
          val left = q
          var index = loop +1
      	  while(index<qualifiers.length){
      		  pairs += (((q,qualifiers(index)),r._2._1))//r._2._1 (ty+MN,value.substring(1)))
  				  index += 1      		  
      	  }
          loop += 1
        }        
        pairs.toIterable
    }.reduceByKey((cty1,cty2)=> cty1+";"+cty2)
     
    val realPair = pairRDD.filter{
      (v) =>
        var keep = false
        val lty = v._1._1.substring(0,2)//左边号码类型
        val l = v._1._1.substring(2)//左边号码
        val rty = v._1._2.substring(0,2)
        val r = v._1._2.substring(2)
        val cells = v._2.split(";")// 连接类型，即通过何种类型的节点连接此号码对，有ID,CI,QQ,WC四种 
        val ctyRaw = cells.map { x => x.substring(0, 2) }
        val cty = ctyRaw.distinct
        val map = new HashMap[String,Int]()
        for(l <- ctyRaw){
      	   if(map.containsKey(l)){
      	     map.put(l, 1+map.get(l))
      	   }else{
      	     map.put(l, 1)
      	   }           
         }
        var idMN = 0        
        if(cty.contains("ID")&&map.get("ID")==1){
           for(c <- cells){
             if(c.substring(0,2).equals("ID")){
               idMN = c.substring(2).toInt//ID231  MN's number            
             }
           }          
        }
        var ciMN = 0        
        if(cty.contains("CI")&&map.get("CI")==1){
           for(c <- cells){
             if(c.substring(0,2).equals("CI")){
               ciMN = c.substring(2).toInt//CI231  MN's number            
             }
           }          
        }
        
        if((!lty.equals(rty))&&l.equals(r)){
          keep = true
        }else{
          //至少需要有一个移动号, 才能比较. 宽带号(可以属于多个用户)之间暂不考虑
          if (lty.equals("MN")||rty.equals("MN")){
             if(cty.contains("QQ")){
               keep = true
             }else{
               if(lty.equals("WM")||lty.equals("AN")||rty.equals("WN")||rty.equals("AN")){
                 if(cty.contains("CI")&&map.get("CI")==1&&ciMN==1&&((!cty.contains("ID"))||(map.get("ID")==1&&idMN==1))){                   
                     keep = true
                 }else{
                   if((!cty.contains("CI")&&map.get("ID")==1&&idMN==1)){
                     keep = true
                   }
                 }
               }
             }
          } 
        }
        keep
    }
    //经过上面步骤过滤，剩下的号码对应该是 AN-WM MN-AN MN-WN MN-MN
    
    val UPTNRDD = keepNodeRDD.map{
      (v) => 
        val row = Bytes.toString(v._2.getRow).substring(2)
        val ty = Bytes.toString(v._2.getRow).substring(2,4)
        if(ty.equals("AN")||ty.equals("WM")||ty.equals("MN")){
          val qualifiers = v._2.rawCells()
          var value = ""
          for(q <- qualifiers){
            val ty = Bytes.toString(q.getQualifier).substring(2,4)
            val qualifier = Bytes.toString(q.getQualifier).substring(2)
            ty match {
						case "ID" => value += ";"+qualifier
						case "CI" => value += ";"+qualifier
						case "QQ" => value += ";"+qualifier
						case "WC" => value += ";"+qualifier
						case "IS" => value += ";"+qualifier
						case "UD" => value += ";"+qualifier
						case _    =>  
    				}
          }
        	(v._1,(row,value.substring(1)))
        }else{
          (0L,("",""))
        }
    }.filter((v) => v._1>0).map((v) => (v._2._1,(v._1,v._2._2))).persist(StorageLevel.MEMORY_AND_DISK)
    
    keepNodeRDD.unpersist(true)
    val nRealPair = realPair.map((v) => (v._1._1,v._1._2))
    //UPTNRDD:(UPTN,SN)                    //  v :(LID, ((LSN, LLinks), RID))=>(RID, (LID,LSN, LLinks)))
    val lJoinRDD = UPTNRDD.join(nRealPair).map((v) => (v._2._2,(v._1,v._2._1._1,v._2._1._2)))
    //output: (lID,(lSN,(rID,rSN)))
    val joinRDD = UPTNRDD.join(lJoinRDD).map{
    // v: (String, ((Long, String), (String, Long, String)))
    // v :(RID, ((RSN, RLinks), (LID,LSN, LLinks)))
      (v) =>
        val leftID = v._1
        val leftSN = v._2._1._1
        val leftLinks = v._2._1._2
        val rightID = v._2._2._1
        val rightSN = v._2._2._2
        val rightLinks = v._2._2._3
        if(leftSN < rightSN){
          ((leftSN,leftID),(rightSN,rightID))
        }else{
          ((rightSN,rightID),(leftSN,leftID))
        }
    }.distinct()
    
    val pairUPTN = joinRDD.flatMap((v) => List[String](v._1._2,v._2._2).toIterable).distinct()
    //notPairUPTN: RDD[(String, (Long, (Long, String)))] => ((SN,ID),(0L,""))
    val notPairUPTN = UPTNRDD.map((v) => v._1).distinct().subtract(pairUPTN).map { x => (x,0L) }.join(UPTNRDD).map((v) => ((v._2._2._1,v._1),(0L,"")))
    UPTNRDD.unpersist(true)
    val realPairs = joinRDD.union(notPairUPTN)
    realPairs   
  }
  
   
}