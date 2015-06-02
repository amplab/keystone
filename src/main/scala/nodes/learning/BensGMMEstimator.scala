package nodes.learning

import breeze.linalg._
import breeze.numerics.{exp, log => bLog}
import breeze.stats._
import org.apache.spark.rdd.RDD
import pipelines.{Logging, Estimator}
import utils.MatrixUtils

/**
 * Created by tomerk11 on 6/1/15.
 */
/**
 * Fit a Gaussian Mixture model to Data.
 *
 * @param k Number of centers to estimate.
 */
case class BensGMMEstimator(k: Int, numIters: Int = 100, minClusterSize: Int = 40, stopTolerance: Double = 1e-4, weightThreshold: Double = 1e-4, smallVarianceThreshold: Double = 1e-2) extends Estimator[DenseVector[Double], DenseVector[Double]] with Logging {

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
    // gather data statistics
    val X = MatrixUtils.rowsToMatrix(samples)
    val numSamples = X.rows
    val numFeatures = X.cols
    val meanGlobal = mean(X(::, *))
    val XSq = X :* X
    val varianceGlobal = mean(XSq(::, *)) - (meanGlobal :* meanGlobal)

    // set the lower bound for the gmm_variance
    val gmmVarLB = smallVarianceThreshold * varianceGlobal

    // Use KMeans++ initialization to get the GMM center initializations
    val kMeansModel = KMeansPlusPlusEstimator(k, 0).fit(samples)
    val centerAssignment = kMeansModel.apply(X)
    val assignMass = sum(centerAssignment, Axis._0)

    var gmmWeights = assignMass / numSamples.toDouble
    var gmmMeans = diag(assignMass.map(1.0 / _)) * (centerAssignment.t * X)
    var gmmVars = diag(assignMass.map(1.0 / _)) * (centerAssignment.t * XSq) - (gmmMeans :* gmmMeans)

    // Threshold small variances
    gmmVars = max(gmmVars, DenseMatrix.ones[Double](k, 1) * gmmVarLB)

    // Run EM!
    val curCost = DenseVector.zeros[Double](numIters)
    var iter = 0
    var costImproving = true
    var largeEnoughClusters = true
    while ((iter < numIters) && costImproving && largeEnoughClusters) {
      // TODO: RUN EM!!
      /* E-STEP */

      /*
      compute the squared malhanobis distance for each gaussian.
      sq_mal_dist(i,j) || x_i - mu_j||_Lambda^2.  I am the master of
      computing Euclidean distances without for loops.
      */
      val sqMahlist = (XSq * gmmVars.map(0.5 / _).t) - (X * (gmmMeans :/ gmmVars).t) + (DenseMatrix.ones[Double](numSamples, 1) * (sum(gmmMeans :* gmmMeans :/ gmmVars, Axis._1).t :* 0.5))

      // compute the log likelihood of the approximate posterior
      var llh = DenseMatrix.ones[Double](numSamples, 1)*(-0.5 * numFeatures * math.log(2*math.Pi) - 0.5 * sum(bLog(gmmVars), Axis._1).t + bLog(gmmWeights)) - sqMahlist

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
        val lseIncrement = (deltaLSE.map(x => if (x > 30.0) 1.0 else 0.0) :* deltaLSE) + (deltaLSE.map(x => if (x > -30.0) 1.0 else 0.0) :* deltaLSE.map(x => if (x <= 30.0) 1.0 else 0.0) :* bLog(exp(deltaLSEThreshold) + 1.0))
        lseLLH = llh(::, cluster) + lseIncrement
        cluster += 1
      }
      curCost(iter) = mean(lseLLH)

      // Check if we're making progress
      if (iter > 0) {
        costImproving = (curCost(iter - 1) - curCost(iter)) >= stopTolerance * math.abs(curCost(iter - 1))
      }

      if (costImproving) {
        /*
        if we make progress, update our pseudo-likelihood for the E-step.
        by shifting the llh to be peaked at 0, we avoid nasty numerical
        overflows.
        */
        llh = llh - (max(llh(*, ::)) * DenseMatrix.ones[Double](1, k))
        var q = exp(llh) :* (sum(exp(llh), Axis._1).map(1.0 / _) * DenseMatrix.ones[Double](1, k))

        /*
        aggressive posterior thresholding: suggested by Xerox.  Thresholds
        the really small weights to sparsify the assignments.
        */
        q = q.map(x => if (x > weightThreshold) x else 0.0)
        q :*= (sum(q, Axis._1).map(1.0 / _) * DenseMatrix.ones[Double](1, k))

        /* M-STEP */
        val qSum = sum(q, Axis._0)
        if (qSum.toArray.exists(_ < 40.0)) {
          logWarning("Unbalanced clustering, try less centers")
          largeEnoughClusters = false
        } else {
          gmmWeights = qSum / numSamples.toDouble
          gmmMeans = diag(qSum.map(1.0 / _)) * (q.t * X)
          gmmVars = diag(qSum.map(1.0 / _)) * (q.t * XSq) - (gmmMeans :* gmmMeans)

          // Threshold small variances
          gmmVars = max(gmmVars, DenseMatrix.ones[Double](k, 1) * gmmVarLB)
        }
      }

      iter += 1
    }

    new GaussianMixtureModel(gmmMeans, gmmVars, gmmWeights.toDenseVector)
  }
}
