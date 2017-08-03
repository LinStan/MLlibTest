package lh



import org.apache.spark._
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random
/**
  * Created by MSIK on 2017/7/31.
  */
/*
object Kmeans {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("csv-kmeans")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val path = args(0)//输出路径
    val input = args(1)//文本路径
    val fs = FileSystem.get(sc.hadoopConfiguration)
    fs.delete(new Path(path),true)
    val rdd = sc.textFile(input).map(
      line => {
        val arr = line.split(",")
        (arr(1).toDouble, arr(2).toDouble)
      })//用，分隔数据

    rdd.cache()

    //K-MEANS
    val K =args(2).toInt//自定义聚类点个数

    var clusters = rdd.takeSample(false,K)
    var totalDistance: Double =0.0//初始化总距离0

    do {
      //广播初始聚类点
      val clusterPointWithIndex = clusters.zipWithIndex.map(_.swap)
      val seeder = sc.broadcast(clusterPointWithIndex)

      //开始计算新的聚类中心
      val newClusterInfo = rdd.map(point => {

        val clusterIndex = seeder.value.map{
          case (index, clusterPoint) => {
            val dist = calcDistance(point, clusterPoint)
            (dist, index)
          }}
          .minBy(_._1)._2
        //返回结果
        (clusterIndex,point)
      })
        .aggregateByKey(ClusterPoint(0D,0D,1L))(
          (clusterPoint,point)=> {
            //累加
            clusterPoint.x += point._1
            clusterPoint.y += point._2
            clusterPoint.n += 1
            //返回聚合结果
            clusterPoint
          },(clusterPoint1,clusterPoint2)=> {
            //累加
            clusterPoint1.x += clusterPoint2.x
            clusterPoint1.y += clusterPoint2.y
            clusterPoint1.n += clusterPoint2.n
            //返回聚合结果
            clusterPoint1
          }
        )
        .collect()
        .map{

          case (oldClusterPoint, clusterPoint)=> {
            //计算新的中心点
            val newCluster = (clusterPoint.x / clusterPoint.n,clusterPoint.y/ clusterPoint.n)
            //获取上一次迭代的质心点
            val oldClusterPoint = clusterPointWithIndex
              .filter(_._1==oldClusterPoint)
              .toList match {
              case (_, point) :: Nil => point
              case _ => throw new RuntimeException("之前的质心点内没有找到")
            }

            //计算老的中心点和新的中心点的距离
            val distance = calcDistance(oldClusterPoint, newCluster)
            //返回新的节点和距离
            (distance, newCluster)
          }
        }

      //清空广播变量
      seeder.unpersist(true)

      //获取总距离
      totalDistance = newClusterInfo.map(_._1).sum
      //新的聚类点集合
      clusters = newClusterInfo.map(_._2)
      println("distance: "+ totalDistance + "," + clusters.mkString("{", ",", "}"))
    }while (totalDistance > 1e-7)

    rdd.map(tuple =>tuple._1 + "\t" + tuple._2).repartition(1).saveAsTextFile(path)
    println("最终质心点：" + clusters.map(_.swap).mkString("{", ",", "}"))

    rdd.unpersist()
  }


  /*
  集群间点的表示方式
   */
  case class ClusterPoint(var x:Double,var y:Double,var n:Long) extends Serializable






  /*
  计算距离
  欧几里得距离
   */
  def calcDistance(point1: (Double, Double),point2: (Double, Double)):Double = {
    import scala.math._
    val a = point1._1 - point2._1
    val b = point1._2 - point2._2
    return sqrt(pow(a,2)+pow(b,2))
  }
}
*/