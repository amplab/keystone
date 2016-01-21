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

package nodes.learning

import breeze.linalg._
import breeze.optimize.{CachedDiffFunction, DiffFunction, LBFGS => BreezeLBFGS}
import edu.berkeley.cs.amplab.mlmatrix.util.{Utils => MLMatrixUtils}
import nodes.stats.StandardScaler
import org.apache.spark.rdd.RDD
import pipelines.Logging
import workflow.LabelEstimator

import scala.collection.mutable

object LBFGSwithL2 extends Logging {
  /**
    * Run Limited-memory BFGS (L-BFGS) in parallel.
    * Averaging the subgradients over different partitions is performed using one standard
    * spark map-reduce in each iteration.
    */
  def runLBFGS[T <: Vector[Double]](
      data: RDD[T],
      labels: RDD[DenseVector[Double]],
      gradient: Gradient[T],
      numCorrections: Int,
      convergenceTol: Double,
      maxNumIterations: Int,
      regParam: Double,
      weightsIncludeBias: Boolean = false): DenseMatrix[Double] = {

    val lossHistory = mutable.ArrayBuilder.make[Double]
    val numExamples = data.count
    val numFeatures = data.first.length
    val numClasses = labels.first.length

    val costFun = new CostFun(data, labels, gradient, regParam, numExamples, numFeatures,
      numClasses)

    val lbfgs = new BreezeLBFGS[DenseVector[Double]](maxNumIterations, numCorrections, convergenceTol)

    val initialWeights = DenseVector.zeros[Double](numFeatures * numClasses)

    val states =
      lbfgs.iterations(new CachedDiffFunction(costFun), initialWeights)

    /**
      * NOTE: lossSum and loss is computed using the weights from the previous iteration
      * and regVal is the regularization value computed in the previous iteration as well.
      */
    var state = states.next()
    while (states.hasNext) {
      lossHistory += state.value
      state = states.next()
    }
    lossHistory += state.value
    val finalWeights = state.x.asDenseMatrix.reshape(numFeatures, numClasses)

    val lossHistoryArray = lossHistory.result()

    logInfo("LBFGS.runLBFGS finished. Last 10 losses %s".format(
      lossHistoryArray.takeRight(10).mkString(", ")))

    finalWeights
  }

  /**
    * CostFun implements Breeze's DiffFunction[T], which returns the loss and gradient
    * at a particular point (weights). It's used in Breeze's convex optimization routines.
    */
  private class CostFun[T <: Vector[Double]](
      data: RDD[T],
      labels: RDD[DenseVector[Double]],
      gradient: Gradient[T],
      regParam: Double,
      numExamples: Long,
      numFeatures: Int,
      numClasses: Int,
      weightsIncludeBias: Boolean = false) extends DiffFunction[DenseVector[Double]] {

    override def calculate(weights: DenseVector[Double]): (Double, DenseVector[Double]) = {
      val weightsMat = weights.asDenseMatrix.reshape(numFeatures, numClasses)
      // Have a local copy to avoid the serialization of CostFun object which is not serializable.
      val bcW = data.context.broadcast(weightsMat)
      val localGradient = gradient
      val localNumFeatures = numFeatures
      val localNumClasses = numClasses

      val partitionGradientLosses = data.zipPartitions(labels) {
        (partitionFeatures, partitionLabels) =>
          Iterator.single(localGradient.compute(localNumFeatures, localNumClasses, partitionFeatures, partitionLabels, bcW.value))
      }

      val (gradientSum, lossSum) = MLMatrixUtils.treeReduce(
        partitionGradientLosses,
        (a: (DenseMatrix[Double], Double), b: (DenseMatrix[Double], Double)) => {
          a._1 += b._1
          (a._1, a._2 + b._2)
        }
      )

      // total loss = lossSum / nTrain + 1/2 * lambda * norm(W)^2
      val normWSquared = if (weightsIncludeBias) {
        math.pow(norm(weights(0 until (weights.length - 1))), 2)
      } else {
        math.pow(norm(weights), 2)
      }
      val regVal = 0.5 * regParam * normWSquared
      val loss = lossSum / numExamples + regVal

      // total gradient = gradSum / nTrain + lambda * w
      val gradientTotal = gradientSum / numExamples.toDouble + (weightsMat * regParam)

      (loss, gradientTotal.toDenseVector)
    }
  }
}

/**
  * :: DeveloperApi ::
  * Class used to solve an optimization problem using Limited-memory BFGS.
  * Reference: [[http://en.wikipedia.org/wiki/Limited-memory_BFGS]]
  * @param gradient Gradient function to be used.
  * @param fitIntercept Whether to fit the intercepts or not.
  * @param numCorrections 3 < numCorrections < 10 is recommended.
  * @param convergenceTol convergence tolerance for L-BFGS
  * @param regParam L2 regularization
  * @param numIterations max number of iterations to run
  */
class DenseLBFGSwithL2(
    val gradient: Gradient.DenseGradient,
    val fitIntercept: Boolean = true,
    val numCorrections: Int = 10,
    val convergenceTol: Double = 1e-4,
    val numIterations: Int = 100,
    val regParam: Double = 0.0)
  extends LabelEstimator[DenseVector[Double], DenseVector[Double], DenseVector[Double]] {

  def fit(data: RDD[DenseVector[Double]], labels: RDD[DenseVector[Double]]): LinearMapper = {
    if (fitIntercept) {
      val featureScaler = new StandardScaler(normalizeStdDev = false).fit(data)
      val labelScaler = new StandardScaler(normalizeStdDev = false).fit(labels)

      val model = LBFGSwithL2.runLBFGS(
        featureScaler.apply(data),
        labelScaler.apply(labels),
        gradient,
        numCorrections,
        convergenceTol,
        numIterations,
        regParam)
      new LinearMapper(model, Some(labelScaler.mean), Some(featureScaler))
    } else {
      val model = LBFGSwithL2.runLBFGS(
        data,
        labels,
        gradient,
        numCorrections,
        convergenceTol,
        numIterations,
        regParam)
      new LinearMapper(model, None, None)
    }
  }
}

/**
  * :: DeveloperApi ::
  * Class used to solve an optimization problem using Limited-memory BFGS.
  * Reference: [[http://en.wikipedia.org/wiki/Limited-memory_BFGS]]
  * @param gradient Gradient function to be used.
  * @param fitIntercept Whether to fit the intercepts or not.
  * @param numCorrections 3 < numCorrections < 10 is recommended.
  * @param convergenceTol convergence tolerance for L-BFGS
  * @param regParam L2 regularization
  * @param numIterations max number of iterations to run
  */
class SparseLBFGSwithL2(
    val gradient: Gradient.SparseGradient,
    val fitIntercept: Boolean = true,
    val numCorrections: Int = 10,
    val convergenceTol: Double = 1e-4,
    val numIterations: Int = 100,
    val regParam: Double = 0.0)
  extends LabelEstimator[SparseVector[Double], DenseVector[Double], DenseVector[Double]] {

  def fit(data: RDD[SparseVector[Double]], labels: RDD[DenseVector[Double]]): SparseLinearMapper = {
    if (fitIntercept) {
      // To fit the intercept, we add a column of ones to the data
      val dataWithOnesColumn = data.map { vec =>
        val out = SparseVector.zeros[Double](vec.length + 1)
        for ((i, v) <- vec.activeIterator) {
          out(i) = v
        }

        out(vec.length) = 1.0
        out
      }

      val model = LBFGSwithL2.runLBFGS(
        dataWithOnesColumn,
        labels,
        gradient,
        numCorrections,
        convergenceTol,
        numIterations,
        regParam,
        weightsIncludeBias = true)

      // We separate the model into the weights and the intercept
      val weights = model(0 until (model.rows - 1), ::).copy
      val intercept = model(model.rows - 1, ::).t

      new SparseLinearMapper(weights, Some(intercept))
    } else {
      val model = LBFGSwithL2.runLBFGS(
        data,
        labels,
        gradient,
        numCorrections,
        convergenceTol,
        numIterations,
        regParam)

      new SparseLinearMapper(model, None)
    }
  }
}
