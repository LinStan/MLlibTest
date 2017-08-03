package lh

import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}
/*
输入参数    训练集路径     测试集路径     中心点数    迭代次数    运行模式
 */
object KMeansClustering {
  def main(args: Array[String]) {

    val conf = new
        SparkConf().setAppName("K-Means Clustering").setMaster(args(4))
    val sc = new SparkContext(conf)

    val rawTrainingData = sc.textFile(args(0))//输入训练集位置
    //将数据以 ， 为分割点，前后都转为double类型的vector集合
    val parsedTrainingData =
    rawTrainingData.filter(!isColumnNameLine(_)).map(line => {
      Vectors.dense(line.split(",").map(_.trim).filter(!"".equals(_)).map(_.toDouble))
    }).cache()

    // Cluster the data into two classes using KMeans
    //质心点
    val numClusters = args(2).toInt
    //迭代次数
    val numIterations = args(3).toInt
    val runTimes = 3
    //质心点索引
    var clusterIndex: Int = 0
    //训练模型参数
    val clusters: KMeansModel =
      KMeans.train(parsedTrainingData, numClusters, numIterations, runTimes)
    //输出质心点数量
     println("Cluster Number:" + clusters.clusterCenters.length)
    //输出每个质心点
    println("Cluster Centers Information Overview:")
    clusters.clusterCenters.foreach(
      x => {
        println("Center Point of Cluster " + clusterIndex + ":")
        println(x)
        clusterIndex += 1
      })

    //begin to check which cluster each test data belongs to based on the clustering result

    //val rawTestData = sc.textFile("file:///Users/lei.wang/data/data_test")
    val rawTestData = sc.textFile(args(1))//输入测试集路径
    //分割测试集数据
    val parsedTestData = rawTestData.map(line => {
      Vectors.dense(line.split(",").map(_.trim).filter(!"".equals(_)).map(_.toDouble))

    })
    //输出测试集聚类结果
    parsedTestData.collect().foreach(testDataLine => {
      val predictedClusterIndex:
        Int = clusters.predict(testDataLine)
      println("The data " + testDataLine.toString + " belongs to cluster " + predictedClusterIndex)


    })
    println("Spark MLlib K-means clustering test finished.")
  }

  private def isColumnNameLine(line: String): Boolean = {
    if (line != null && line.contains("Channel")) true
    else false
  }
}