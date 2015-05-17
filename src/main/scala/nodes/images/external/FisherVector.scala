package nodes.images.external

import breeze.linalg._
import nodes.images.FisherVectorInterface
import nodes.learning.GaussianMixtureModel
import org.apache.spark.rdd.RDD
import utils.external.EncEval

/**
 * Implements a wrapper for the `enceval` Fisher Vector implementation.
 *
 * @param gmm A trained Gaussian Mixture Model
 */
class FisherVector(
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
