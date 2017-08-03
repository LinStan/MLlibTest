package lh

/**
  * Created by MSIK on 2017/7/24.
  * 获取年龄文本平均数
  */
import org.apache.spark._
object AvgAgeCalculator {
  def main(args: Array[String]) {
    if (args.length<1)
    {
      println("No Found DataFile")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Ave Age")
    val sc = new SparkContext(conf)
    val datafile = sc.textFile(args(0),5);//获取数据路径
    val count = datafile.count()
    val ageData = datafile.map(line => line.split(" ")(1))//数据分行，一行一个数据
    val total = ageData.map(age => Integer.parseInt(
                                  String.valueOf(age))).collect().reduce((a,b) =>a+b)
    println("Total Age="+total+";Number of people="+count)
    val avgAge : Double=total.toDouble / count.toDouble
    println("Avg = "+avgAge)
  }

}
