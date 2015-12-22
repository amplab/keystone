package nodes.learning.external

import breeze.linalg.{convert, DenseMatrix, DenseVector}
import nodes.learning.GaussianMixtureModel
import org.apache.spark.rdd.RDD
import utils.external.EncEval
import workflow.Estimator

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
