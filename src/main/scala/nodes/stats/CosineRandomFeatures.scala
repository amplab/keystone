package nodes.stats

import breeze.linalg._
import breeze.numerics._
import breeze.stats.distributions.Rand
import org.apache.spark.rdd.RDD
import pipelines._
import utils.MatrixUtils
import workflow.Transformer

/**
 * Transformer that extracts random cosine features from a feature vector
 * @param W A matrix of dimension (# output features) by (# input features)
 * @param b a dense vector of dimension (# output features)
 *
 * Transformer maps vector x to cos(x * transpose(W) + b).
 * Kernel trick to allow Linear Solver to learn cosine interaction terms of the input
 */
class CosineRandomFeatures(
  @transient val W: DenseMatrix[Double], // should be numOutputFeatures by numInputFeatures
  @transient val b: DenseVector[Double]) // should be numOutputFeatures by 1
  extends Transformer[DenseVector[Double], DenseVector[Double]] {

  require(b.length == W.rows, "# of rows in W and size of b should match")
  override def apply(in: RDD[DenseVector[Double]]): RDD[DenseVector[Double]] = {
    val wBroadcast = in.sparkContext.broadcast(W)
    val bBroadcast = in.sparkContext.broadcast(b)
    in.mapPartitions { part =>
      val data = MatrixUtils.rowsToMatrix(part)
      val features: DenseMatrix[Double] = data * wBroadcast.value.t
      features(*,::) :+= bBroadcast.value
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
  /** Generate Random Cosine Features from the given distributions **/
  def apply(
      numInputFeatures: Int,
      numOutputFeatures: Int,
      gamma: Double,
      wDist: Rand[Double] = Rand.gaussian,
      bDist: Rand[Double] = Rand.uniform) = {
    val W = DenseMatrix.rand(numOutputFeatures, numInputFeatures, wDist) :* gamma
    val b = DenseVector.rand(numOutputFeatures, bDist) :* (2*math.Pi)
    new CosineRandomFeatures(W, b)
  }
}
