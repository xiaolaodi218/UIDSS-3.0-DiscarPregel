import java.text.SimpleDateFormat
import java.util.Date

/**
  * Created by Administrator on 2017/11/16.
  */
object Test3 {
  def main(args: Array[String]): Unit = {
    val timestamp = getTimestamp("12345678901234")
    print(timestamp)
  }
  def info(e: =>String): Any = {
    print(e)
  }
  def getTimestamp(x: String): Date = {
    val format = new SimpleDateFormat("yyyyMMddHHmmss")
    try {
      val d = format.parse(x);
      return d
    } catch {
      case e: Exception => println("Get timestamp wrong!")
    }
    return null
  }
}

