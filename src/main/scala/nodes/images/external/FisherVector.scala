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
    @transient centroid_means: DenseMatrix[Double],
    @transient centroid_variances: DenseMatrix[Double],
    @transient centroid_priors: Array[Double])

  extends FisherVectorInterface {

  val numDims = centroid_means.rows
  val numCentroids = centroid_means.cols
  val numFeatures = numDims * numCentroids * 2

  private def fv(ins: Iterator[DenseMatrix[Double]],
      means: Array[Float],
      vars: Array[Float],
      wts: Array[Float]): Iterator[DenseMatrix[Double]] = {

    val extLib = new EncEval

    ins.map(in => {
      val fisherVector = extLib.calcAndGetFVs(means, numDims, numCentroids,
        vars, wts, in.toArray.map(_.toFloat))

      convert(new DenseMatrix(numDims, numCentroids*2, fisherVector), Double)
    })
  }

  def apply(in: RDD[DenseMatrix[Double]]): RDD[DenseMatrix[Double]] = {
    val sc = in.context

    val means = sc.broadcast(convert(centroid_means, Float).toArray)
    val vars = sc.broadcast(convert(centroid_variances, Float).toArray)
    val wts = sc.broadcast(centroid_priors.map(_.toFloat))

    in.mapPartitions(x => fv(x, means.value, vars.value, wts.value))
  }

  override def apply(in: DenseMatrix[Double]): DenseMatrix[Double] = {
    val means = convert(centroid_means, Float).toArray
    val vars = convert(centroid_variances, Float).toArray
    val wts = centroid_priors.map(_.toFloat)

    val extLib = new EncEval

    val fisherVector = extLib.calcAndGetFVs(means, numDims, numCentroids,
      vars, wts, in.toArray.map(_.toFloat))

    convert(new DenseMatrix(numDims, numCentroids*2, fisherVector), Double)
  }
}
