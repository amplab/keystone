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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import breeze.linalg._
import breeze.numerics._
import breeze.math._
import breeze.stats._
import breeze.optimize.{CachedDiffFunction, DiffFunction, LBFGS => BreezeLBFGS}

import edu.berkeley.cs.amplab.mlmatrix.util.{Utils => MLMatrixUtils}

import org.apache.spark.rdd.RDD

import workflow.LabelEstimator
import nodes.stats.{StandardScaler, StandardScalerModel}
import pipelines.Logging
import utils.{MatrixUtils, Stats}

/**
 * :: DeveloperApi ::
 * Class used to solve an optimization problem using Limited-memory BFGS.
 * Reference: [[http://en.wikipedia.org/wiki/Limited-memory_BFGS]]
 * @param gradient Gradient function to be used.
 * @param numCorrections 3 < numCorrections < 10 is recommended.
 * @param convergenceTol convergence tolerance for L-BFGS
 * @param regParam L2 regularization
 * @param numIterations max number of iterations to run 
 */
class LBFGSwithL2(
  val gradient: BatchGradient,
  val numCorrections: Int  = 10,
  val convergenceTol: Double = 1e-4,
  val numIterations: Int = 100,
  val regParam: Double = 0.0)
  extends LabelEstimator[DenseVector[Double], DenseVector[Double], DenseVector[Double]] {

  def fit(data: RDD[DenseVector[Double]], labels: RDD[DenseVector[Double]]): LinearMapper = {
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
  }

}

object LBFGSwithL2 extends Logging {
  /**
   * Run Limited-memory BFGS (L-BFGS) in parallel.
   * Averaging the subgradients over different partitions is performed using one standard
   * spark map-reduce in each iteration.
   */
  def runLBFGS(
    data: RDD[DenseVector[Double]],
    labels: RDD[DenseVector[Double]],
    gradient: BatchGradient,
    numCorrections: Int,
    convergenceTol: Double,
    maxNumIterations: Int,
    regParam: Double): DenseMatrix[Double] = {

    val lossHistory = mutable.ArrayBuilder.make[Double]
    val numExamples = data.count
    val numFeatures = data.first.length
    val numClasses = labels.first.length

    val dataMat = data.mapPartitions { part =>
      Iterator.single(MatrixUtils.rowsToMatrix(part))
    }

    val labelsMat = labels.mapPartitions { part =>
      Iterator.single(MatrixUtils.rowsToMatrix(part))
    }

    val costFun = new CostFun(dataMat, labelsMat, gradient, regParam, numExamples, numFeatures,
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
  private class CostFun(
    dataMat: RDD[DenseMatrix[Double]],
    labelsMat: RDD[DenseMatrix[Double]],
    gradient: BatchGradient,
    regParam: Double,
    numExamples: Long,
    numFeatures: Int,
    numClasses: Int) extends DiffFunction[DenseVector[Double]] {

    override def calculate(weights: DenseVector[Double]): (Double, DenseVector[Double]) = {
      val weightsMat = weights.asDenseMatrix.reshape(numFeatures, numClasses)
      // Have a local copy to avoid the serialization of CostFun object which is not serializable.
      val bcW = dataMat.context.broadcast(weightsMat)
      val localGradient = gradient

      val (gradientSum, lossSum) = MLMatrixUtils.treeReduce(dataMat.zip(labelsMat).map { x =>
        localGradient.compute(x._1, x._2, bcW.value)
      }, (a: (DenseMatrix[Double], Double), b: (DenseMatrix[Double], Double)) => {
        a._1 += b._1
        (a._1, a._2 + b._2)
      }
      )

      // total loss = lossSum / nTrain + 1/2 * lambda * norm(W)^2
      val normWSquared = math.pow(norm(weights), 2)
      val regVal = 0.5 * regParam * normWSquared
      val loss = lossSum / numExamples + regVal

      // total gradient = gradSum / nTrain + lambda * w
      val gradientTotal = gradientSum / numExamples.toDouble + (weightsMat * regParam)

      (loss, gradientTotal.toDenseVector)
    }
  }
}
