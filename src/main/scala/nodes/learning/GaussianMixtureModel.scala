package nodes.learning

import java.io.File

import breeze.linalg._
import breeze.numerics._
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
case class GaussianMixtureModel(
    means: DenseMatrix[Double],
    variances: DenseMatrix[Double],
    weights: DenseVector[Double],
    weightThreshold: Double = 1e-4)
  extends Transformer[DenseVector[Double],DenseVector[Double]] {

  private val gmmMeans = means.t
  private val gmmVars = variances.t
  private val gmmWeights = weights.toDenseMatrix

  val k = means.cols
  val dim = means.rows

  require(means.rows == variances.rows && means.cols == variances.cols, "GMM means and variances must be the same size.")
  require(weights.length == k, "Every GMM center must have a weight.")

  /**
   * Returns (thresholded) assignments to each cluster.
   * @param in A Vector
   * @return The thresholded assignments of the vector according to the mixture model.
   */
  def apply(in: DenseVector[Double]): DenseVector[Double] = {
    // TODO: Could maybe do more efficient single-item implementation
    apply(in.asDenseMatrix).toDenseVector
  }

  def apply(X: DenseMatrix[Double]): DenseMatrix[Double] = {
    // gather data statistics
    val numSamples = X.rows
    val numFeatures = X.cols
    val XSq = X :* X

    /*
    compute the squared malhanobis distance for each gaussian.
    sq_mal_dist(i,j) || x_i - mu_j||_Lambda^2.  I am the master of
    computing Euclidean distances without for loops.
    */
    val sqMahlist = (XSq * gmmVars.map(0.5 / _).t) - (X * (gmmMeans :/ gmmVars).t) + (DenseMatrix.ones[Double](numSamples, 1) * (sum(gmmMeans :* gmmMeans :/ gmmVars, Axis._1).t :* 0.5))

    // compute the log likelihood of the approximate posterior
    val llh = DenseMatrix.ones[Double](numSamples, 1) * (-0.5 * numFeatures * math.log(2 * math.Pi) - 0.5 * sum(log(gmmVars), Axis._1).t + log(gmmWeights)) - sqMahlist

    /*
    if we make progress, update our pseudo-likelihood for the E-step.
    by shifting the llh to be peaked at 0, we avoid nasty numerical
    overflows.
    */
    llh(::, *) -= max(llh(*, ::))
    exp.inPlace(llh)
    llh(::, *) :/= sum(llh, Axis._1)
    var q = llh

    /*
    aggressive posterior thresholding: suggested by Xerox.  Thresholds
    the really small weights to sparsify the assignments.
    */
    q = q.map(x => if (x > weightThreshold) x else 0.0)
    q(::, *) :/= sum(q, Axis._1)

    q
  }

  override def apply(in: RDD[DenseVector[Double]]): RDD[DenseVector[Double]] = {
    in.mapPartitions { partition =>
      val assignments = apply(MatrixUtils.rowsToMatrix(partition))
      MatrixUtils.matrixToRowArray(assignments).iterator
    }
  }
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