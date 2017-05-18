package com.spark.ml

import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by AnLei on 2017/5/17.
  */
object TfIdfExample {

  def main(args: Array[String]) {

    val spark = SparkSession.builder().master("local").appName("TfIdfExample").getOrCreate()

    // $example on$
    val sentenceData = spark.createDataFrame(Seq(
      (0.0, "Hi I heard about Spark"),
      (0.0, "I wish Java could use case classes"),
      (1.0, "Logistic regression models are neat")
    )).toDF("label", "sentence")

    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val wordsData = tokenizer.transform(sentenceData)
    wordsData.select("label", "sentence", "words").show()

    val hashingTF = new HashingTF()
      .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)

    val featurizedData = hashingTF.transform(wordsData)
    featurizedData.select("label", "sentence", "words", "rawFeatures").show()
    // alternatively, CountVectorizer can also be used to get term frequency vectors

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)

    val rescaledData = idfModel.transform(featurizedData)
    rescaledData.select("label", "sentence", "words", "rawFeatures", "features").show()
    // $example off$

    rescaledData.write.mode(SaveMode.Overwrite).json("output/TfIdf.json")

    spark.stop()
  }

}
