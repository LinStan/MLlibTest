package lh

import java.io.FileWriter
import java.io.File

import scala.util.Random
/*
代码生成1000000位人口年龄数据文件
 */
object SampleDataFileGenerator{
  def main(args: Array[String]): Unit = {
    val write = new FileWriter(new File("D:\\age.txt"),false)
    val rand = new Random()//0-100伪随机
    for(i<-1 to 10000){
      write.write(i+" "+rand.nextInt(100))
      write.write(System.getProperty("line.separator"))

    }
    write.flush()
    write.close()
  }
}