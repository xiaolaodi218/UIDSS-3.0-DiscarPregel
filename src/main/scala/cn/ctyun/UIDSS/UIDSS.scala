/*********************************************************************
 * 
 * CHINA TELECOM CORPORATION CONFIDENTIAL
 * ______________________________________________________________
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
 **********************************************************************/

package cn.ctyun.UIDSS

/**
 * UID Service On Spark
 * @author hongjie zhou
 */

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.util.Properties

import utils.{ Utils, Logging, Hash }
import cmds._

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import java.util.concurrent.{Executors, ExecutorService}

object UIDSS extends Logging {

  val APP_PROP_NAME = "user-id-server.properties"; //集群属性文件  

  def main(args: Array[String]) {
    
    if (args.length < 1) {
      println("USAGE: spark-submit [options] <app jar | python file> [app options]".format())
      System.exit(0)
    }
    var cmd = args(0)

    val props = new Properties()
    props.load(this.getClass.getClassLoader().getResourceAsStream(APP_PROP_NAME));

    val conf = new SparkConf()
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.shuffle.blockTransferService", "nio")
    conf.set("spark.shuffle.manager", "sort")

    if (cmd.substring(0, 2).compareTo("L_") == 0) {
      conf.setMaster("local[2]").setAppName("UIDSS-Local")
      cmd = cmd.substring(2)

    } else if (cmd.substring(0, 2).compareTo("Y_") == 0) { //run on remote server
      conf.setMaster("yarn-cluster").setAppName("UIDSS_On_Yarn")
      cmd = cmd.substring(2)
    } else { //run on remote server
      conf.setAppName("UIDSS")
    }
    
    val sc = new SparkContext(conf)
    
    //监控线程
    Executors.newSingleThreadExecutor().execute(new MonitorThread(sc))

    
    cmd match {
      case "BatchQuery"     => BatchQueryCmd.execute(sc, props)
      case "GenerateUID"    => GenUIDCmd.execute(sc, props)
      case "GenerateUIDExt" => GenUID3Cmd.execute(sc, props, args(1))
      case "LoadRawData" => {
        if (args.length < 3) {
          println("There should be at least 3 arguments! ")
          info("There should be at least 3 arguments! ")
        } else {
          if (args.length > 4) {
            LoadRawDataCmd.execute(sc, props, args(1), args(2), args(3), args(4))
          } else {
            LoadRawDataCmd.execute(sc, props, args(1), args(2), args(3), "")
          }
        }
      }
      case "DeleteOldData" => {
        if (args.length < 2 || args(1).length!=14) {
          println("Please give the date in 'yyyymmddhhmmss' format ! ")
          info("Please give the date in 'yyyymmddhhmmss' format ! ")
        } else {
          val oldTime = getTimestamp(args(1))
          DeleteOldDataCmd.execute(sc, props, oldTime)
        }
      }
      case "Stat" => Stat.execute(sc, props, args(1))
      case _ => info("Not a valid command!")
    }
    
    println("Program finished !")
    sc.stop()
  }

  def getTimestamp(x: String): Long = {
    val format = new SimpleDateFormat("yyyyMMddHHmmss")
    try {
        val d = format.parse(x);
        return d.getTime()
    } catch {
      case e: Exception => println("Get timestamp wrong!")
    }
    return 0L
  }
}

class MonitorThread(sc: SparkContext) extends Runnable {
  override def run() {
    var timeTooFewExector = 0
    var timeExit = 0
    while (true) {
      val iActiveJob = sc.statusTracker.getActiveJobIds().length
      if (iActiveJob > 0) {
        if (sc.getExecutorMemoryStatus.size < 2) {
          timeTooFewExector += 1
          if (timeTooFewExector > 600) {
            //只有driver在运行，超过60秒
            println("************UIDSS  is shut down, because too few executors! ****************")
            sc.stop()
            return
          }
        } else {
          timeTooFewExector = 0
        }
      } else {
        //没有活动job，超过300秒
        timeExit += 1
        if (timeExit > 300) {
            return
         }
      }
      Thread.sleep(1000)
    }
  }
}

