package lh

import java.util.Random
import java.lang.Math._
import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector

/*
 *将数据文本内的数据进行k-means聚类分析
 * 正则匹配模式为\t
 * 运行时格式 spark-submit --master local（运行模式）--class lh.SparkCore类名和包名 jar包路径。本地    文本文档路径。hdfs    输出结果路径。hdfs
 */
object SparkCore {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("kmeans in Spark")
    val sc = new SparkContext(conf)
    val input = args(0) //输入数据
    val output = args(1) //输出路径
    val k = args(2).toInt //聚类个数
    var s = 0d //聚类效果评价标准
    val shold = 0.1 //收敛阀值
    var s1 = Double.MaxValue
    var times = 0
    var readyForIteration = true
    val func1 = (x: (newVector, Int, Double), y: (newVector, Int, Double)) => {
      (x._1 + y._1, x._2 + y._2, x._3 + y._3)}

    val points = sc.textFile(input).map(line => {
      val temp = line.split("\t").map(ele => ele.toDouble)
      Vectors.dense(temp)}).cache() //将输入数据转换成RDD
    var centers = points.takeSample(false, k, new Random().nextLong()) //生成随机初始质心
    print("------------------------------------------------\n")
    print("Print the centers for the next iteration: \n")
    printCenters(centers)
    print("Ready for the next iteration ? "+readyForIteration+"\n")
    while (readyForIteration) {
      times += 1
      print("Print the result of the clustering in iteration "+times+"\n")
      val reClusteringResult = points.map(v => {
        val (centerId, minDistance) = getClosestCenter(centers, v)
        print("Cluster id: "+centerId+", ")
        print("The point in the cluster "+centerId+": ")
        v.toArray.foreach(x => print(x+","));print("\n")
        (centerId, (newVector(v), 1, minDistance))})
      val NewCentersRdd = reClusteringResult.reduceByKey(func1(_,_))
        .map(ele => {
          val centerId = ele._1
          val newCenter = (ele._2)._1 * (1d / ele._2._2)
          val sumOfDistance = (ele._2)._3
          (newCenter.point, sumOfDistance)})
      var s2 = getNewCenters(NewCentersRdd, centers)
      s = abs(s2 - s1)
      print("s = "+s+"\n")
      print("------------------------------------------------\n")
      print("Print the centers for the next iteration: \n")
      printCenters(centers)
      if (s <= shold) {
        readyForIteration = false
        reClusteringResult.map(ele => {
          var centerId = ele._1.toString()+"\t"
          val array = ele._2._1.point.toArray
          for (i <- 0 until array.length) {
            if (i == array.length - 1)
              {centerId = centerId + array(i).toString()}
            else
              {centerId = centerId + array(i).toString() + "\t"}
          }
          centerId
        }).saveAsTextFile(output) //如果算法收敛，输出结果
      }
      print("to the next iteration ? "+readyForIteration+"\n")
      s1 = s2
    }
    sc.stop()
  }

  case class newVector(point: Vector) {
    def *(a: Double): newVector = {
      var res = new Array[Double](point.size)
      for (i <- 0 until point.size) {
        res(i) = a*point.toArray.apply(i)
      }
      newVector(Vectors.dense(res))
    }
    def +(that: newVector): newVector = {
      var res = new Array[Double](point.size)
      for (i <- 0 until point.size) {
        res(i) = point.toArray.apply(i) + that.point.toArray.apply(i)
      }
      newVector(Vectors.dense(res))
    }
    def -(that: newVector): newVector = {
      this + (that * -1)
    }
    def pointLength(): Double = {
      var res = 0d
      for (i <- 0 until point.size) {
        res = res + pow(point.toArray.apply(i), 2)
      }
      res
    }
    def distanceTo(that: newVector): Double = {
      (this - that).pointLength()
    }
  }

  implicit def toNewVector(point: Vector) = newVector(point)

  def getClosestCenter(centers: Array[Vector], point: Vector): (Int, Double) = {
    var minDistance = Double.MaxValue
    var centerId = 0
    for (i <- 0 until centers.length) {
      if (point.distanceTo(centers(i)) < minDistance) {
        minDistance = point.distanceTo(centers(i))
        centerId = i
      }
    }
    (centerId, minDistance)
  }

  def getNewCenters(rdd: RDD[(Vector, Double)], centers: Array[Vector]): Double ={
    val res = rdd.take(centers.length)
    var sumOfDistance = 0d
    for (i <- 0 until centers.length) {
      centers(i) = res.apply(i)._1
      sumOfDistance += res.apply(i)._2
    }
    sumOfDistance
  }

  def printCenters(centers: Array[Vector]) {
    for (v <- centers) {
      v.toArray.foreach(x => print(x+","));print("\n")
    }
  }
}
