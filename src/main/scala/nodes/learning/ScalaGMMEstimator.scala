package nodes.learning

import breeze.linalg._
import breeze.numerics.{exp, log => bLog}
import breeze.stats._
import org.apache.spark.rdd.RDD
import pipelines.Logging
import utils.MatrixUtils
import workflow.Estimator

/**
 * Fit a Gaussian Mixture model to Data.
 *
 * @param k Number of centers to estimate.
 */
case class ScalaGMMEstimator(
    k: Int,
    maxIterations: Int = 100,
    minClusterSize: Int = 40,
    stopTolerance: Double = 1e-4,
    weightThreshold: Double = 1e-4,
    smallVarianceThreshold: Double = 1e-2,
    absoluteVarianceThreshold: Double = 1e-9)
  extends Estimator[DenseVector[Double], DenseVector[Double]] with Logging {
  require(minClusterSize > 0, "Minimum cluster size must be positive")
  require(maxIterations > 0, "maxIterations must be positive")

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
    require(samples.length > 0, "Must have training points")

    val X = MatrixUtils.rowsToMatrix(samples)

    // Use KMeans++ initialization to get the GMM center initializations
    val kMeansModel = KMeansPlusPlusEstimator(k, 1).fit(X)
    val centerAssignment = kMeansModel.apply(X)
    val assignMass = sum(centerAssignment, Axis._0).toDenseVector

    // gather data statistics
    val numSamples = X.rows
    val numFeatures = X.cols
    val meanGlobal = mean(X(::, *))
    val XSq = X :* X
    val varianceGlobal = mean(XSq(::, *)) - (meanGlobal :* meanGlobal)

    // set the lower bound for the gmm_variance
    val gmmVarLB = max(smallVarianceThreshold * varianceGlobal, absoluteVarianceThreshold)

    var gmmWeights = assignMass.asDenseMatrix / numSamples.toDouble
    var gmmMeans = diag(assignMass.map(1.0 / _)) * (centerAssignment.t * X)
    var gmmVars = diag(assignMass.map(1.0 / _)) * (centerAssignment.t * XSq) - (gmmMeans :* gmmMeans)

    // Threshold small variances
    gmmVars = max(gmmVars, DenseMatrix.ones[Double](k, 1) * gmmVarLB)

    // Run EM!
    val curCost = DenseVector.zeros[Double](maxIterations)
    var iter = 0
    var costImproving = true
    var largeEnoughClusters = true
    while ((iter < maxIterations) && costImproving && largeEnoughClusters) {
      /* E-STEP */

      /*
      compute the squared malhanobis distance for each gaussian.
      sq_mal_dist(i,j) || x_i - mu_j||_Lambda^2.
      */
      val sqMahlist = (XSq * gmmVars.map(0.5 / _).t) - (X * (gmmMeans :/ gmmVars).t) +
          (DenseMatrix.ones[Double](numSamples, 1) * (sum(gmmMeans :* gmmMeans :/ gmmVars, Axis._1).t :* 0.5))

      // compute the log likelihood of the approximate posterior
      val llh = DenseMatrix.ones[Double](numSamples, 1) *
          (-0.5 * numFeatures * math.log(2 * math.Pi) - 0.5 * sum(bLog(gmmVars), Axis._1).t + bLog(gmmWeights)) -
          sqMahlist

      /*
      compute the log likelihood of the model using the incremental
      approach suggested by the Xerox folks.  The key thing here is that
      for all intents and purposes, log(1+exp(t)) is equal to zero is t<-30
      and equal to t if t>30
      */
      var lseLLH = llh(::, 0)
      var cluster = 1
      while (cluster < k) {
        val deltaLSE = lseLLH - llh(::, cluster)
        val deltaLSEThreshold = min(max(deltaLSE, -30.0), 30.0)
        val lseIncrement = (deltaLSE.map(x => if (x > 30.0) 1.0 else 0.0) :* deltaLSE) + (
            deltaLSE.map(x => if (x > -30.0) 1.0 else 0.0) :*
            deltaLSE.map(x => if (x <= 30.0) 1.0 else 0.0) :*
            bLog(exp(deltaLSEThreshold) + 1.0)
        )
        lseLLH = llh(::, cluster) + lseIncrement
        cluster += 1
      }
      curCost(iter) = mean(lseLLH)
      logInfo(s"iter=$iter, llh=${curCost(iter)}")
      // Check if we're making progress
      if (iter > 0) {
        costImproving = (curCost(iter) - curCost(iter - 1)) >= stopTolerance * math.abs(curCost(iter - 1))
      }
      logInfo(s"cost improving: $costImproving")

      if (costImproving) {
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

        /* M-STEP */
        val qSum = sum(q, Axis._0).toDenseVector
        if (qSum.toArray.exists(_ < minClusterSize)) {
          logWarning("Unbalanced clustering, try less centers")
          largeEnoughClusters = false
        } else {
          gmmWeights = qSum.asDenseMatrix / numSamples.toDouble
          gmmMeans = diag(qSum.map(1.0 / _)) * (q.t * X)
          gmmVars = diag(qSum.map(1.0 / _)) * (q.t * XSq) - (gmmMeans :* gmmMeans)

          // Threshold small variances
          gmmVars = max(gmmVars, DenseMatrix.ones[Double](k, 1) * gmmVarLB)
        }
      }

      iter += 1
    }

    GaussianMixtureModel(gmmMeans.t, gmmVars.t, gmmWeights.toDenseVector)
  }
}
