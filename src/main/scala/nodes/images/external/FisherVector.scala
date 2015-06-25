package nodes.images.external

import breeze.linalg._
import nodes.images.FisherVectorInterface
import nodes.learning.{GaussianMixtureModelEstimator, GaussianMixtureModel}
import org.apache.spark.rdd.RDD
import utils.MatrixUtils
import utils.external.EncEval
import workflow.{Transformer, Estimator}

/**
 * Implements a wrapper for the `enceval` Fisher Vector implementation.
 *
 * @param gmm A trained Gaussian Mixture Model
 */
case class FisherVector(
    gmm: GaussianMixtureModel)
  extends FisherVectorInterface {

  @transient lazy val extLib = new EncEval()

  val numDims = gmm.means.rows
  val numCentroids = gmm.means.cols
  val numFeatures = numDims * numCentroids * 2

  override def apply(in: DenseMatrix[Float]): DenseMatrix[Float] = {
    val means = convert(gmm.means, Float).toArray
    val vars = convert(gmm.variances, Float).toArray
    val wts = convert(gmm.weights, Float).toArray

    val fisherVector = extLib.calcAndGetFVs(means, numDims, numCentroids,
      vars, wts, in.toArray)

    new DenseMatrix(numDims, numCentroids*2, fisherVector)
  }
}

/**
 * Trains an `enceval` Fisher Vector implementation, via
 * estimating a GMM by treating each column of the inputs as a separate
 * DenseVector input to [[GaussianMixtureModelEstimator]]
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