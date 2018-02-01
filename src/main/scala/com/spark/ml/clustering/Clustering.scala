package com.spark.ml.clustering

import com.spark.ml.util.{MLUtils, TrainingUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.clustering.KMeans

/**
  * Created by AnLei on 2017/11/14.
  *
  * Clustering
  */
object Clustering {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName(s"${this.getClass.getSimpleName}").getOrCreate()

    val numFeatures = 50
    /**
      * 训练集
      */
    val clusteringDataFrame = spark.createDataFrame(TrainingUtils.clusteringData).toDF("label", "text") // clusteringData2
    /**
      * 分词,向量化
      */
    val clustering = MLUtils.hashingFeatures(clusteringDataFrame, numFeatures).select("label", "features")

    /**
      * K-means模型
      */
    val kmeans = new KMeans().setK(3).setSeed(1L) // .setInitMode("k-means||")
    val model = kmeans.fit(clustering)

    /**
      * 聚类中心
      */
    model.clusterCenters.foreach(println)
    /**
      * 聚类结果
      */
    model.transform(clustering).show(false)

    spark.stop()
  }
}
