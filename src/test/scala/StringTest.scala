object StringTest {
  def main(args: Array[String]): Unit = {
    val list2 = List(1,2,3)

  }

  def testVariable(ars:String*): Unit ={
    var i = 0
    for (ar <- ars){
      println("arg["+i+"] is "+ar)
      i=i+1
    }

  }

}
