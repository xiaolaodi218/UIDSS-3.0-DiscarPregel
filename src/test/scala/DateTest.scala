import java.text.SimpleDateFormat

object DateTest {
  def main(args: Array[String]): Unit = {
    val l = getTimestamp("20170221134242")
    print(l)

  }
  def getTimestamp(x: String): Long = {
    val format = new SimpleDateFormat("yyyyMMddHHmmss")
    try {
      val d = format.parse(x);
      return d.getTime
    } catch {
      case e: Exception => println("Get timestamp wrong!")
    }
    return 0L
  }
}
