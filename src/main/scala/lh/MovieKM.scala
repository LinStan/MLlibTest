package lh

import breeze.linalg._
import breeze.numerics.pow
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by MSIK on 2017/8/2.
  * kmeans聚类ml-100k电影数据集
  * 选出每一类的前20部高分电影
  */
object MovieKM {


  /**
    * 输入参数，第一个：master运行模式
    */

    def main(args: Array[String]) {

      val sparkConf = new SparkConf().setAppName("MovieKM").setMaster(args(0));
      val sc = new SparkContext(sparkConf);
      val movies = sc.textFile("hdfs://wgp-00:9000/lin/input/ml-100k/u.item")
      println("====================================================================")
      println(movies.first)
      // 1|Toy Story (1995)|01-Jan-1995||http://us.imdb.com/M/title-exact?Toy%20Story%20(1995)|0|0|0|1|1|1|0|0|0|0|0|0|0|0|0|0|0|0|0
      val genres = sc.textFile("hdfs://wgp-00:9000/lin/input/ml-100k/u.genre")
      //输出五个标签
      genres.take(5).foreach(println)
      //输出map后的结果
      val genreMap = genres.filter(!_.isEmpty).map(_.split("\\|")).map(array => (array(1),array(0))).collectAsMap();
      println(genreMap)

      //为电影数据和题材映射关系创建新的RDD，其中包含电影ID、标题和题材
      val titlesAndGenres = movies.map(_.split("\\|")).map{array =>
        val genres = array.toSeq.slice(5,array.size)
        val genresAssigned = genres.zipWithIndex.filter{case(g,id) =>
          g == '1'//挑出是1的类型，即选出Action
        }.map{case(g,id)=>
          genreMap(id.toString);
        }
        (array(0).toInt,(array(1),genresAssigned))
      }
      //println(titlesAndGenres.first())

      //Run ALS model to generate movie and user factors
      val rawData = sc.textFile("hdfs://wgp-00:9000/lin/input/ml-100k/u.data")
      val rawRatings = rawData.map(_.split("\t").take(3))
      val ratings = rawRatings.map{ case Array(user, movie, rating) => Rating(user.toInt, movie.toInt, rating.toDouble) }
      ratings.cache();
      val alsModel = ALS.train(ratings,50,10,0.01);
      //最小二乘法（Alternating Least Squares，ALS）模型返回了两个键值RDD（user-Features和productFeatures）。这两个RDD的键为用户ID或者电影ID，值为相关因素。
      //提取相关的因素并转化到MLlib的Vector中作为聚类模型的训练输入
      val movieFactors = alsModel.productFeatures.map{case(id,factor) => (id,Vectors.dense(factor))};
      val movieVectors = movieFactors.map(_._2)//即得到上一个式子中的Vectors.dense(factor)

      val userFactors = alsModel.userFeatures.map { case (id, factor) => (id, Vectors.dense(factor)) }
      val userVectors = userFactors.map(_._2)

      //归一化数据
      val movieMatrix = new RowMatrix(movieVectors);
      val userMatrix = new RowMatrix(userVectors);
      val movieMatrixSummary = movieMatrix.computeColumnSummaryStatistics();
      val userMatrixSummary = userMatrix.computeColumnSummaryStatistics();
      //计算标准差，方差
      println("Movie factors mean: \n" + movieMatrixSummary.mean )
      println("Movie factors variance: \n" + movieMatrixSummary.variance)
      println("User factors mean: \n" + userMatrixSummary.mean)
      println("User factors variance: \n" + userMatrixSummary.variance)
      //正确


      //训练聚类模型 k-聚类
      val numClusters = 5//K值
      val numIterations = 10//迭代次数
      val numRuns = 3//训练次数

      //对电影的系数向量运行K-均值算法
      val movieClusterModel = KMeans.train(movieVectors,numClusters,numIterations,numRuns);
      //用户相关因素的特征向量上训练K-均值模型
      val userClusterModel = KMeans.train(userVectors,numClusters,numIterations,numRuns);

      //使用训练的K-均值模型进行预测
      val movie1 = movieVectors.first
      val movieCluster = movieClusterModel.predict(movie1);
      println("movieCluster："+ movieCluster);

      val predictions = movieClusterModel.predict(movieVectors)
      println("predict："+predictions.take(10).mkString(","))

      //正确

      //-均值最小化的目标函数是样本到其类中心的欧拉距离之和
      //对每个电影计算其特征向量与所属类簇中心向量的距离
      val titlesWithFactors = titlesAndGenres.join(movieFactors)

      val moviesAssigned = titlesWithFactors.map { case (id, ((title, genres), vector)) =>
        val pred = movieClusterModel.predict(vector)
        val clusterCentre = movieClusterModel.clusterCenters(pred)

        val dist = computeDistance(DenseVector(clusterCentre.toArray), DenseVector(vector.toArray))
        (id, title, genres.mkString(" "), pred, dist)
      }//电影的信息为：电影ID、标题、题材、类别索引，以及电影的特征向量和类中心的距离
      val clusterAssignments = moviesAssigned.groupBy { case (id, title, genres, cluster, dist) => cluster }.collectAsMap//根据特征向量进行分组
      //枚举每个类簇并输出距离类中心最近的前20部电影
      for((k,v) <- clusterAssignments.toSeq.sortBy(_._1))
      {
        println(s"Cluster $k:")
        val m = v.toSeq.sortBy(_._5)//根据距离排序
        println(m.take(20).map{case(_,title,genres,_,d) => (title,genres,d)}.mkString("\n"))
        println("======================================================")
      }

      //MLlib提供的函数computeCost可以方便地计算出给定输入数据RDD [Vector]的WCSS
      val movieCost = movieClusterModel.computeCost(movieVectors);
      val userCost = userClusterModel.computeCost(userVectors);
      println("WCSS for movies: " + movieCost)
      println("WCSS for users: " + userCost)
      // WCSS for movies: 2586.0777166339426
      // WCSS for users: 1403.4137493396831

      //聚类模型参数调优,对于k-means，即改变k值
      //交叉验证，电影聚类
      val trainTestSplitMovies = movieVectors.randomSplit(Array(0.6,0.4),123)
      val trainMovies = trainTestSplitMovies(0)//训练集
      val testMovies = trainTestSplitMovies(1)//测试集
      val costsMovies = Seq(2, 3, 4, 5, 10, 20).map{k=>
        (k,KMeans.train(trainMovies,k,numIterations,numRuns).computeCost(testMovies));
      }
      println("Movie clustering cross-validation:")
      costsMovies.foreach { case (k, cost) => println(f"WCSS for K=$k id $cost%2.2f") }

      /* Movie clustering cross-validation:
      WCSS for K=2 id 942.06
      WCSS for K=3 id 942.67
      WCSS for K=4 id 950.35
      WCSS for K=5 id 948.20
      WCSS for K=10 id 943.26
      WCSS for K=20 id 947.10
      */




      //用户聚类
      val trainTestSplitUsers = userVectors.randomSplit(Array(0.6, 0.4), 123)
      val trainUsers = trainTestSplitUsers(0)
      val testUsers = trainTestSplitUsers(1)
      val costsUsers = Seq(2, 3, 4, 5, 10, 20).map { k => (k, KMeans.train(trainUsers, numIterations, k, numRuns).computeCost(testUsers)) }
      println("User clustering cross-validation:")
      costsUsers.foreach { case (k, cost) => println(f"WCSS for K=$k id $cost%2.2f") }
      /* User clustering cross-validation:
      WCSS for K=2 id 544.02
      WCSS for K=3 id 542.18
      WCSS for K=4 id 542.38
      WCSS for K=5 id 542.33
      WCSS for K=10 id 539.68
      WCSS for K=20 id 541.21
      */


    }


    def computeDistance(v1: DenseVector[Double],v2: DenseVector[Double]):Double=pow(v1 - v2, 2).sum


}
