package nodes.learning

import breeze.linalg._
import breeze.numerics.sqrt
import breeze.stats.mean
import nodes.images.FisherVectorInterface
import pipelines.Transformer

/**
 * Implements a fisher vector.
 *
 * @param gmm A trained Gaussian Mixture Model
 */
case class BensFisherVector(gmm: GaussianMixtureModel)
    extends FisherVectorInterface {

  private val gmmMeans = gmm.means
  private val gmmVars = gmm.variances
  private val gmmWeights = gmm.weights

  /**
   *
   * @param in  matrix of size numSiftDimensions by numSiftDescriptors
   * @return  The output value
   */
  override def apply(in: DenseMatrix[Float]): DenseMatrix[Float] = {
    val nDesc = in.cols.toDouble

    // Get the fisher vector posterior assignments
    val x = convert(in, Double)
    val q = gmm.apply(x.t) // numSiftDescriptors x K

    /* here is the Fisher Vector in all of its beauty.  This is directly
    from the FV survey by Sanchez et al: */
    val s0 = mean(q, Axis._0).toDenseVector // 1 x K, but really K x 1 because it's a dense vector
    val s1 = (x * q) :/= nDesc // D x K
    val s2 = ((x :* x) * q) :/= nDesc // D x K

    val fv1 = (s1 - gmmMeans * diag(s0)) :/ (sqrt(gmmVars) * diag(sqrt(gmmWeights)))
    val fv2 = (s2 - (gmmMeans * 2.0 :* s1) + (((gmmMeans :* gmmMeans) - gmmVars)*diag(s0))) :/ (gmmVars * diag(sqrt(gmmWeights :* 2.0)))

    // concatenate the two fv terms
    convert(DenseMatrix.horzcat(fv1, fv2), Float)
  }
}
