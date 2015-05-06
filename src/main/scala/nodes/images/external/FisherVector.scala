package nodes.images.external

import breeze.linalg._
import nodes.images.FisherVectorInterface
import org.apache.spark.rdd.RDD
import utils.external.EncEval

/**
 * Implements a wrapper for the `enceval` Fisher Vector implementation.
 *
 * @param centroid_means GMM Means.
 * @param centroid_variances GMM Variances.
 * @param centroid_priors GMM Priors.
 */
class FisherVector(
    centroid_means: DenseMatrix[Double],
    centroid_variances: DenseMatrix[Double],
    centroid_priors: Array[Double])

  extends FisherVectorInterface {

  @transient lazy val extLib = new EncEval()

  val numDims = centroid_means.rows
  val numCentroids = centroid_means.cols
  val numFeatures = numDims * numCentroids * 2

  override def apply(in: DenseMatrix[Double]): DenseMatrix[Double] = {
    val means = convert(centroid_means, Float).toArray
    val vars = convert(centroid_variances, Float).toArray
    val wts = centroid_priors.map(_.toFloat)

    val fisherVector = extLib.calcAndGetFVs(means, numDims, numCentroids,
      vars, wts, in.toArray.map(_.toFloat))

    convert(new DenseMatrix(numDims, numCentroids*2, fisherVector), Double)
  }
}
