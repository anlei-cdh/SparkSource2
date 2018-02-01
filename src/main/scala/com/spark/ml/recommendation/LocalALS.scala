package com.spark.ml.recommendation

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.commons.math3.linear._

/**
  * Alternating least squares matrix factorization.
  *
  * This is an example implementation for learning how to use Spark. For more conventional use,
  * please refer to org.apache.spark.ml.recommendation.ALS.
  */
object LocalALS {

  // Parameters set through command line arguments
  var M = 0 // Number of movies
  var U = 0 // Number of users
  var F = 0 // Number of features
  var ITERATIONS = 0
  val LAMBDA = 0.01 // Regularization coefficient

  def generateR(): RealMatrix = {
    val mh = randomMatrix(M, F)
    val uh = randomMatrix(U, F)
    mh.multiply(uh.transpose())
  }

  def rmse(targetR: RealMatrix, ms: Array[RealVector], us: Array[RealVector]): Double = {
    val r = new Array2DRowRealMatrix(M, U)
    for (i <- 0 until M; j <- 0 until U) {
      r.setEntry(i, j, ms(i).dotProduct(us(j)))
    }
    val diffs = r.subtract(targetR)
    var sumSqs = 0.0
    for (i <- 0 until M; j <- 0 until U) {
      val diff = diffs.getEntry(i, j)
      sumSqs += diff * diff
    }
    math.sqrt(sumSqs / (M.toDouble * U.toDouble))
  }

  /**
    * 固定其中一个用户来更新所有的电影向量
    * @param i 第几个
    * @param m 相当于第几个电影
    * @param us 用户矩阵
    * @param R 评分矩阵
    * @return
    */
  def updateMovie(i: Int, m: RealVector, us: Array[RealVector], R: RealMatrix) : RealVector = {
    var XtX: RealMatrix = new Array2DRowRealMatrix(F, F) // 矩阵
    var Xty: RealVector = new ArrayRealVector(F) // 向量
    // For each user that rated the movie
    /**
      * 迭代用户矩阵
      */
    for (j <- 0 until U) {
      val u = us(j) // 单个用户
      // Add u * u^t to XtX
      XtX = XtX.add(u.outerProduct(u)) // 用户矩阵
      // Add u * rating to Xty
      Xty = Xty.add(u.mapMultiply(R.getEntry(i, j))) // 评分矩阵 第i个电影的所有用户评分
    }
    // Add regularization coefficients to diagonal terms
    for (d <- 0 until F) {
      XtX.addToEntry(d, d, LAMBDA * U)
    }
    // Solve it with Cholesky
    new CholeskyDecomposition(XtX).getSolver.solve(Xty) // 矩阵分解
  }

  def updateUser(j: Int, u: RealVector, ms: Array[RealVector], R: RealMatrix) : RealVector = {
    var XtX: RealMatrix = new Array2DRowRealMatrix(F, F)
    var Xty: RealVector = new ArrayRealVector(F)
    // For each movie that the user rated
    for (i <- 0 until M) {
      val m = ms(i)
      // Add m * m^t to XtX
      XtX = XtX.add(m.outerProduct(m))
      // Add m * rating to Xty
      Xty = Xty.add(m.mapMultiply(R.getEntry(i, j)))
    }
    // Add regularization coefficients to diagonal terms
    for (d <- 0 until F) {
      XtX.addToEntry(d, d, LAMBDA * M)
    }
    // Solve it with Cholesky
    new CholeskyDecomposition(XtX).getSolver.solve(Xty)
  }

  def showWarning() {
    System.err.println(
      """WARN: This is a naive implementation of ALS and is given as an example!
        |Please use org.apache.spark.ml.recommendation.ALS
        |for more conventional use.
      """.stripMargin)
  }

  def main(args: Array[String]) {

    args match {
      case Array(m, u, f, iters) =>
        M = m.toInt
        U = u.toInt
        F = f.toInt
        ITERATIONS = iters.toInt
      case _ =>
        System.err.println("Usage: LocalALS <M> <U> <F> <iters>")
        System.exit(1)
    }

    showWarning()

    println(s"Running with M=$M, U=$U, F=$F, iters=$ITERATIONS")

    val R = generateR()

    // Initialize m and u randomly
    var ms = Array.fill(M)(randomVector(F)) // 电影矩阵 M电影的数量 F相当于K值
    var us = Array.fill(U)(randomVector(F)) // 用户矩阵 U用户的数量 F相当于K值

    // Iteratively update movies then users
    for (iter <- 1 to ITERATIONS) { // ITERATIONS 迭代次数
      println(s"Iteration $iter:")
      ms = (0 until M).map(i => updateMovie(i, ms(i), us, R)).toArray // ms(i)相当于第几个电影
      us = (0 until U).map(j => updateUser(j, us(j), ms, R)).toArray // us(j)相当于第几个用户
      println("RMSE = " + rmse(R, ms, us))
      println()
    }
  }

  private def randomVector(n: Int): RealVector =
    new ArrayRealVector(Array.fill(n)(math.random))

  private def randomMatrix(rows: Int, cols: Int): RealMatrix =
    new Array2DRowRealMatrix(Array.fill(rows, cols)(math.random))

}