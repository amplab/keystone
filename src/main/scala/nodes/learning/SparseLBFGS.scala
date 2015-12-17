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
import org.apache.spark.mllib.feature.{StandardScaler => MLLibStandardScaler}

import workflow.{LabelEstimator, Transformer}
import nodes.stats.{StandardScaler, StandardScalerModel}
import pipelines.Logging
import utils.{MatrixUtils, Stats}
import utils.MLlibUtils.{breezeVectorToMLlib}

/**
 * Computes A * x + b i.e. a linear map of data using a trained model.
 *
 * @param x trained model
 * @param bOpt optional intercept to add
 */
case class SparseLinearMapper(
    x: DenseMatrix[Double],
    bOpt: Option[DenseVector[Double]] = None)
  extends Transformer[SparseVector[Double], DenseVector[Double]] {

  /**
   * Apply a linear model to an input.
   * @param in Input.
   * @return Output.
   */
  def apply(in: SparseVector[Double]): DenseVector[Double] = {
    val out = x.t * in
    val bout = bOpt.map { b =>
      out :+= b
    }.getOrElse(out)

    if (bout.length == 1) {
      val boutEx = (DenseVector.ones[Double](2) * -1.0)
      if (bout(0) > 0) {
        boutEx(1) = 1.0
      } else {
        boutEx(0) = 1.0
      }
      boutEx
    } else {
      out
    }
  }

  /**
   * Apply a linear model to a collection of inputs.
   *
   * @param in Collection of A's.
   * @return Collection of B's.
   */
  override def apply(in: RDD[SparseVector[Double]]): RDD[DenseVector[Double]] = {
    val modelBroadcast = in.context.broadcast(x)
    val bBroadcast = in.context.broadcast(bOpt)
    in.map(row => {
      val out = modelBroadcast.value.t * row
      val bout = bOpt.map { b =>
        out :+= b
      }.getOrElse(out)

      if (bout.length == 1) {
        val boutEx = (DenseVector.ones[Double](2) * -1.0)
        if (bout(0) > 0) {
          boutEx(1) = 1.0
        } else {
          boutEx(0) = 1.0
        }
        boutEx
      } else {
        out
      }
    })
  }
}

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
class SparseLBFGSwithL2(
    val gradient: SparseGradient,
    val normalizeStdDev: Boolean,
    val numClasses: Int,
    val numCorrections: Int  = 10,
    val convergenceTol: Double = 1e-4,
    val numIterations: Int = 100,
    val regParam: Double = 0.0)
    extends LabelEstimator[SparseVector[Double], DenseVector[Double], Int] {

  def fit(data: RDD[SparseVector[Double]], labels: RDD[Int]): SparseLinearMapper = {

    val dataVec = if (!normalizeStdDev) {
      data
    } else {
      val scaler = new MLLibStandardScaler(withStd = true, withMean = false).fit(data.map(x =>
        breezeVectorToMLlib(x)))
      data.map { vec =>
        val scaled = scaler.transform(
          breezeVectorToMLlib(vec)).asInstanceOf[org.apache.spark.mllib.linalg.SparseVector]
        new SparseVector[Double](scaled.indices, scaled.values, scaled.size)
      }
    }

    // Convert labels to +1, -1
    val labelsVec = if (numClasses == 2) {
      labels.map { x =>
        val out = DenseVector.ones[Double](1)
        out(0) = 2.0*x - 1.0
        out
      }
    } else {
      labels.map { x =>
        assert(x < numClasses)
        val out = DenseVector.ones[Double](numClasses) * -1.0
        out(x) = 1.0
        out
      }
    }

    val model = SparseLBFGSwithL2.runLBFGS(
      dataVec,
      labelsVec,
      gradient,
      numCorrections,
      convergenceTol,
      numIterations,
      regParam,
      numClasses)

    new SparseLinearMapper(model, None)
  }

}

object SparseLBFGSwithL2 extends Logging {
  /**
   * Run Limited-memory BFGS (L-BFGS) in parallel.
   * Averaging the subgradients over different partitions is performed using one standard
   * spark map-reduce in each iteration.
   */
  def runLBFGS(
    data: RDD[SparseVector[Double]],
    labels: RDD[DenseVector[Double]],
    gradient: SparseGradient,
    numCorrections: Int,
    convergenceTol: Double,
    maxNumIterations: Int,
    regParam: Double,
    numClasses: Int): DenseMatrix[Double] = {

    val lossHistory = mutable.ArrayBuilder.make[Double]
    val numExamples = data.count
    val numFeatures = data.first.size

    val costFun = new CostFun(data, labels, gradient, regParam, numExamples, numFeatures,
      numClasses)

    val lbfgs = new BreezeLBFGS[DenseVector[Double]](maxNumIterations, numCorrections, convergenceTol)

    val initialWeights = if (numClasses == 2) {
      DenseVector.zeros[Double](numFeatures)
    } else {
      DenseVector.zeros[Double](numFeatures * numClasses)
    }

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
    val finalWeights = if (numClasses == 2) {
      state.x.asDenseMatrix.reshape(numFeatures, 1)
    } else {
      state.x.asDenseMatrix.reshape(numFeatures, numClasses)
    }

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
    data: RDD[SparseVector[Double]],
    labels: RDD[DenseVector[Double]],
    gradient: SparseGradient,
    regParam: Double,
    numExamples: Long,
    numFeatures: Int,
    numClasses: Int) extends DiffFunction[DenseVector[Double]] {

    override def calculate(weights: DenseVector[Double]): (Double, DenseVector[Double]) = {

      val actualNumClasses = if (numClasses == 2) 1 else numClasses

      val weightsMat = weights.asDenseMatrix.reshape(numFeatures, actualNumClasses)
      // Have a local copy to avoid the serialization of CostFun object which is not serializable.
      val bcW = data.context.broadcast(weightsMat)
      val localGradient = gradient

      val (gradientSum, lossSum) = data.zip(labels).treeAggregate(
        (DenseMatrix.zeros[Double](numFeatures, actualNumClasses), 0.0))(
        seqOp = (c, v) => (c, v) match { case ((grad, loss), (feature, label)) =>
          val l = localGradient.compute(feature, label, bcW.value, grad)
          (grad, loss + l)
        },
        combOp = (c1, c2) => (c1, c2) match { case ((grad1, loss1), (grad2, loss2)) =>
          grad1 += grad2
          (grad1, loss1 + loss2)
        })

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
