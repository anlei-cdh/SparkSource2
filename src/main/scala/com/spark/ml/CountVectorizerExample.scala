package com.spark.ml

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}

/**
  * Created by AnLei on 2017/5/18.
  */
object CountVectorizerExample {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName("Word2VecExample").getOrCreate()

    val df = spark.createDataFrame(Seq(
      (0, Array("a", "b", "c")),
      (1, Array("a", "b", "b", "c", "a"))
    )).toDF("id", "words")

    // fit a CountVectorizerModel from the corpus
    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
      .setVocabSize(3)
      .setMinDF(2)
      .fit(df)

    // alternatively, define CountVectorizerModel with a-priori vocabulary
    val cvm = new CountVectorizerModel(Array("a", "b", "c"))
      .setInputCol("words")
      .setOutputCol("features")

    cvModel.transform(df).show(false)
  }

}
