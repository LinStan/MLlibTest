package lh

/**
  * Created by MSIK on 2017/7/20.
  * 聚类分析国足在亚洲排名
  * 输入参数： 数据文本路径
  * 文本数据如下
  */
  /*
  参考博客  http://www.cnblogs.com/leoo2sk/archive/2010/09/20/k-means.html
 */
  /*
  50 50 9
  28 09 4
  17 15 3
  25 40 5
  28 40 2
  50 50 1
  50 40 9
  50 40 9
  40 40 5
  50 50 9
  50 50 5
  50 50 9
  40 40 9
  40 32 17
  50 50 9
   */

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering.KMeans
object KMSoccer extends App{

  override def main(args: Array[String]): Unit = super.main(args)

    val conf = new SparkConf()
    conf.setAppName("KMSoccer").setMaster("local")
  //直接设定本地运行

    val sc = new SparkContext(conf)
    val rawtxt = sc.textFile(args(0))

    // 将文本文件的内容转化为 Double 类型的 Vector 集合
    val allData = rawtxt.map {
      line =>
        Vectors.dense(line.split(' ').map(_.toDouble))
    }

    // 由于 K-Means 算法是迭代计算，这里把数据缓存起来（广播变量）
    allData.cache()
    // 分为 3 个子集，最多50次迭代
    val kMeansModel = KMeans.train(allData, 3, 50)
    // 输出每个子集的质心
    kMeansModel.clusterCenters.foreach {
      println
    }

    val kMeansCost = kMeansModel.computeCost(allData)

    // 输出本次聚类操作的收敛性，此值越低越好
    println("K-Means Cost: " + kMeansCost)

    // 输出每组数据及其所属的子集索引
    allData.foreach {
      vec =>
        println(kMeansModel.predict(vec) + ": " + vec)
    }


}
