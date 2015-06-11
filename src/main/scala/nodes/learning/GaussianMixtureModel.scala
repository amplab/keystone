package nodes.learning

import java.io.File

import breeze.linalg._
import org.apache.spark.rdd.RDD
import pipelines._
import utils.MatrixUtils
import utils.external.EncEval
import workflow.{Transformer, Estimator}

/**
 * A Mixture of Gaussians, usually computed via some clustering process.
 *
 * @param means Cluster centers.
 * @param variances Cluster variances (diagonal)
 * @param weights Cluster weights.
 */
class GaussianMixtureModel(
    val means: DenseMatrix[Double],
    val variances: DenseMatrix[Double],
    val weights: DenseVector[Double])
  extends Transformer[DenseVector[Double],DenseVector[Double]] with Logging {

  val k = means.cols
  val dim = means.rows

  require(means.rows == variances.rows && means.cols == variances.cols, "GMM means and variances must be the same size.")
  require(weights.length == k, "Every GMM center must have a weight.")

  /**
   * For now this is unimplemented. It should return the soft assignment to each cluster.
   * @param in A Vector
   * @return The soft assignments of the vector according to the mixture model.
   */
  override def apply(in: DenseVector[Double]): DenseVector[Double] = ???
}


/**
 * Fit a Gaussian Mixture model to Data.
 *
 * @param k Number of centers to estimate.
 */
class GaussianMixtureModelEstimator(k: Int) extends Estimator[DenseVector[Double], DenseVector[Double]] {

  /**
   * Currently this model works on items that fit in local memory.
   * @param samples
   * @return A PipelineNode (Transformer) which can be called on new data.
   */
  def fit(samples: RDD[DenseVector[Double]]): GaussianMixtureModel = {
    fit(samples.collect)
  }

  /**
   * Fit a Gaussian mixture model with `k` centers to a sample array.
   *
   * @param samples Sample Array - all elements must be the same size.
   * @return A Gaussian Mixture Model.
   */
  def fit(samples: Array[DenseVector[Double]]): GaussianMixtureModel = {
    val extLib = new EncEval
    val nDim = samples(0).length

    // Flatten this thing out.
    val sampleFloats = samples.map(_.toArray.map(_.toFloat))
    val res = extLib.computeGMM(k, nDim, sampleFloats.flatten)

    val meanSize = k*nDim
    val varSize = k*nDim
    val coefSize = k*nDim

    // Each array region is expected to be centroid-major.
    val means = convert(new DenseMatrix(nDim, k, res.slice(0, meanSize)), Double)
    val vars = convert(new DenseMatrix(nDim, k, res.slice(meanSize, meanSize+varSize)), Double)
    val coefs = convert(new DenseVector(res.slice(meanSize+varSize, meanSize+varSize+coefSize)), Double)

    new GaussianMixtureModel(means, vars, coefs)
  }
}

object GaussianMixtureModel {
  def load(meanFile: String, varsFile: String, weightsFile: String): GaussianMixtureModel = {

    val means = csvread(new File(meanFile))
    val variances = csvread(new File(varsFile))
    val weights = csvread(new File(weightsFile)).toDenseVector

    new GaussianMixtureModel(means, variances, weights)
  }
}