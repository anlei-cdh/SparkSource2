package com.spark.ml.clustering

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum

object ClusteringTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName(s"${this.getClass.getSimpleName}").getOrCreate()

    val df = spark.createDataFrame(
      Seq(
        (125, List(8.0,8.0,1.0,8.0), 1),
        (124, List(1.0,2.0,6.0,2.0,3.0), 1),
        (123, List(1.0,1.0,6.0,3.0,3.0), 2),
        (122, List(1.0,1.0,6.0,5.0,1.0), 2),
        (121, List(1.0,4.0,6.0,4.0,1.0), 3)
      )
    ).toDF("label", "textlist", "prediction")

    df.show(false)

    // df.groupBy("prediction").count().show(false)
    /**
      * [8.0, 8.0, 1.0, 8.0] ->
      * 8.0
      * 8.0
      * 1.0
      * 8.0
      */
    val explode = df.selectExpr("prediction", "explode(textlist) AS text")

    explode.show(false)

    /**
      * 聚类结果和频道标签分组
      */
    val groupTextCount = explode.groupBy("prediction","text").count().selectExpr("prediction","cast(text as int)","count")
    groupTextCount.show(false)

    /**
      * 只按聚类结果分组 相当于标签数据加和
      * 增加频道标签列为了union
      */
    val groupCount = groupTextCount.groupBy("prediction").agg(sum("count") as "count")
    val groupCountAddText = groupCount.selectExpr("prediction", "(prediction * 0) as text", "count")
    groupCountAddText.show(false)

    groupTextCount.union(groupCountAddText).show(100)

    spark.stop()
  }

}
