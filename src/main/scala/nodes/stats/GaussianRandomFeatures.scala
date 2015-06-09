package nodes.stats

import breeze.linalg.{max, *, DenseVector, DenseMatrix}
import breeze.numerics.sqrt
import breeze.stats.distributions.Rand
import nodes.learning.ZCAWhitener
import pipelines.Transformer

/**
 * @param W A matrix of dimension (# output features) by (# input features)
 * @param b a dense vector of dimension (# output features)
 *
 * Transformer maps matrix x to (transpose(W) * x + b, 0).
 */
class MatrixLinearMapperThenMax(
   val W: DenseMatrix[Double],
   val b: DenseVector[Double],
   val threshold: Option[Double] = None                                )
  extends Transformer[DenseMatrix[Double], DenseMatrix[Double]] {

  /**
   * Apply a linear model to an input.
   * @param in Input.
   * @return Output.
   */
  def apply(in: DenseMatrix[Double]): DenseMatrix[Double] = {
    val features: DenseMatrix[Double] = W.t * in
    features(::,*) :+= b
    threshold.foreach { x =>
      features :-= x
    }
    max(features, 0.0)
  }
}

object GaussianRandomFeatures {
  /** Generate Random Gaussian Features from the given distributions **/
  def apply(
    numGaussianFilters: Int,
    zcaWhitener: ZCAWhitener,
    wDist: Rand[Double] = Rand.gaussian) = {
    val zca = zcaWhitener.whitener
    val W = zca * DenseMatrix.rand(zca.cols, numGaussianFilters, wDist) :/ sqrt(zca.cols.toDouble) // numZCADimensions by numGaussianFilters
    val b = zcaWhitener.means.t * W
    b :*= -1.0
    new MatrixLinearMapperThenMax(W, b.t)
  }
}