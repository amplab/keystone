package nodes.misc

import breeze.linalg._
import breeze.numerics._
import breeze.stats.distributions.RandBasis
import org.apache.spark.rdd.RDD
import pipelines._
import utils.{MatrixUtils, Stats}

/**
 * Transformer that extracts random cosine features from a feature vector
 * @param W A matrix of dimension (# output features) by (# input features)
 * @param b a dense vector of dimension (# output features)
 *
 * Transformer maps vector x to cos(x * transpose(W) + b).
 * Kernel trick to allow Linear Solver to learn cosine interaction terms of the input
 */
class CosineRandomFeatures(
    val W: DenseMatrix[Double], // should be numOutputFeatures by numInputFeatures
    val b: DenseVector[Double]) // should be numOutputFeatures by 1
    extends Transformer[DenseVector[Double], DenseVector[Double]] {

  override def apply(in: RDD[DenseVector[Double]]): RDD[DenseVector[Double]] = {
    in.mapPartitions { part =>
      val arr = part.toArray
      val numRows = arr.length
      val numCols = arr(0).length
      val matData = new Array[Double](numRows * numCols)
      var i = 0
      while (i < arr.length) {
        val row = arr(i)
        var j = 0
        while (j < numCols) {
          matData(i + numRows * j) = row(j)
          j = j + 1
        }
        i = i + 1
      }
      val data = new DenseMatrix[Double](numRows, numCols, matData)
      val features: DenseMatrix[Double] = data * W.t
      features(*,::) :+= b
      cos.inPlace(features)
      MatrixUtils.matrixToRowArray(features).iterator
    }
  }

  override def apply(in: DenseVector[Double]): DenseVector[Double] = {
    val features = (in.t * W.t).t
    features :+= b
    cos.inPlace(features)
    features
  }
}

/**
 * Companion Object to generate random cosine features from various distributions
 */
object CosineRandomFeatures {
  /** Generate Random Cosine Features from a gaussian distribution **/
  def createGaussianCosineRF(
      numInputFeatures: Int,
      numOutputFeatures: Int,
      gamma: Double,
      rand: RandBasis) = {
    val W = Stats.randMatrixGaussian(numOutputFeatures, numInputFeatures, rand) :* gamma
    val b = Stats.randMatrixUniform(numOutputFeatures, 1, rand) :* (2*math.Pi)
    new CosineRandomFeatures(W, b.toDenseVector)
  }

  /** Generate Random Cosine Features from a cauchy distribution **/
  def createCauchyCosineRF(
      numInputFeatures: Int,
      numOutputFeatures: Int,
      gamma: Double,
      rand: RandBasis) = {
    val W = Stats.randMatrixCauchy(numOutputFeatures, numInputFeatures, rand) :* gamma
    val b = Stats.randMatrixUniform(numOutputFeatures, 1, rand) :* (2*math.Pi)
    new CosineRandomFeatures(W, b.toDenseVector)
  }
}
