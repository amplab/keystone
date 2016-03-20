package nodes.images

import breeze.linalg._
import breeze.numerics._
import breeze.stats.mean
import nodes.learning.{GaussianMixtureModelEstimator, GaussianMixtureModel}
import org.apache.spark.rdd.RDD
import utils.MatrixUtils
import workflow.{Pipeline, OptimizableEstimator, Estimator, Transformer}

/**
 * Abstract interface for Fisher Vector.
 */
trait FisherVectorInterface extends Transformer[DenseMatrix[Float], DenseMatrix[Float]]

/**
 * Implements a fisher vector.
 *
 * @param gmm A trained Gaussian Mixture Model
 */
case class FisherVector(gmm: GaussianMixtureModel)
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
    val fv2 = (s2 - (gmmMeans * 2.0 :* s1) + (((gmmMeans :* gmmMeans) - gmmVars)*diag(s0))) :/
        (gmmVars * diag(sqrt(gmmWeights :* 2.0)))

    // concatenate the two fv terms
    convert(DenseMatrix.horzcat(fv1, fv2), Float)
  }
}

/**
 * Trains a scala Fisher Vector implementation, via
 * estimating a GMM by treating each column of the inputs as a separate
 * DenseVector input to [[GaussianMixtureModelEstimator]]
 *
 * TODO: Pending philosophical discussions on how to best make it so you can
 * swap in GMM, KMeans++, etc. for Fisher Vectors. For now just hard-codes GMM here
 *
 * @param k Number of centers to estimate.
 */
case class GMMFisherVectorEstimator(k: Int) extends Estimator[DenseMatrix[Float], DenseMatrix[Float]] {
  protected def fit(data: RDD[DenseMatrix[Float]]): FisherVector = {
    val gmmTrainingData = data.flatMap(x => convert(MatrixUtils.matrixToColArray(x), Double))
    val gmmEst = new GaussianMixtureModelEstimator(k)
    val gmm = gmmEst.fit(gmmTrainingData)
    FisherVector(gmm)
  }
}

/**
 * Trains either a scala or an `enceval` Fisher Vector implementation, via
 * estimating a GMM by treating each column of the inputs as a separate
 * DenseVector input to [[GaussianMixtureModelEstimator]]
 *
 * Automatically decides which implementation to use when node-level optimization is enabled.
 *
 * @param k Number of centers to estimate.
 */

case class OptimizableGMMFisherVectorEstimator(k: Int) extends OptimizableEstimator[DenseMatrix[Float], DenseMatrix[Float]] {
  val default = GMMFisherVectorEstimator(k)

  override def optimize(sample: RDD[DenseMatrix[Float]], numPerPartition: Map[Int, Int])
  : RDD[DenseMatrix[Float]] => Pipeline[DenseMatrix[Float], DenseMatrix[Float]] = {
    if (k >= 32) {
      nodes.images.external.GMMFisherVectorEstimator(k).withData(_)
    } else {
      GMMFisherVectorEstimator(k).withData(_)
    }
  }
}