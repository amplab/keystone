package nodes.learning

import breeze.linalg._
import breeze.numerics.{exp, log => bLog}
import breeze.stats._
import breeze.stats.distributions.{Uniform, ThreadLocalRandomGenerator, RandBasis, Rand}
import org.apache.commons.math3.random.MersenneTwister
import org.apache.spark.rdd.RDD
import pipelines.Logging
import utils.MatrixUtils
import workflow.Estimator

/**
 * Fit a Gaussian Mixture model to Data.
 * We assume diagonal covariances.
 *
 * Trains the GMM using the guidelines described in Appendix B of:
 *
 * Jorge Sanchez, Florent Perronnin, Thomas Mensink, Jakob Verbeek. Image Classification with
 * the Fisher Vector: Theory and Practice. International Journal of Computer Vision, Springer
 * Verlag, 2013, 105 (3), pp.222-245. <10.1007/s11263-013-0636-x>.
 *
 * @param k Number of centers to estimate.
 */
case class GaussianMixtureModelEstimator(
    k: Int,
    maxIterations: Int = 100,
    minClusterSize: Int = 40,
    stopTolerance: Double = 1e-4,
    weightThreshold: Double = 1e-4,
    smallVarianceThreshold: Double = 1e-2,
    absoluteVarianceThreshold: Double = 1e-9,
    initializationMethod: GMMInitializationMethod = KMEANS_PLUS_PLUS_INITIALIZATION,
    seed: Int = 0)
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

    // gather data statistics
    val numSamples = X.rows
    val numFeatures = X.cols
    val meanGlobal = mean(X(::, *))
    val XSq = X :* X
    val varianceGlobal = mean(XSq(::, *)) - (meanGlobal :* meanGlobal)

    var (gmmWeights, gmmMeans, gmmVars) = initializationMethod match {
      case KMEANS_PLUS_PLUS_INITIALIZATION =>
        // Use KMeans++ initialization to get the GMM center initializations
        val kMeansModel = KMeansPlusPlusEstimator(k, 1, seed = seed).fit(X)
        val centerAssignment = kMeansModel.apply(X)
        val assignMass = sum(centerAssignment, Axis._0).toDenseVector

        val gmmWeights = assignMass.asDenseMatrix / numSamples.toDouble
        val gmmMeans = diag(assignMass.map(1.0 / _)) * (centerAssignment.t * X)
        val gmmVars = diag(assignMass.map(1.0 / _)) * (centerAssignment.t * XSq) - (gmmMeans :* gmmMeans)

        (gmmWeights, gmmMeans, gmmVars)

      case RANDOM_INITIALIZATION =>
        // Random Initialization
        val colMin = min(X(::, *)).toDenseVector
        val colMax = max(X(::, *)).toDenseVector
        val colRange = colMax - colMin

        val rand = new RandBasis(new ThreadLocalRandomGenerator(new MersenneTwister(seed)))

        val gmmMeans = DenseMatrix.rand(k, numFeatures, Uniform(0, 1)(rand))
        gmmMeans(*, ::) :*= colRange
        gmmMeans(*, ::) :+= colMin

        val gmmVars = DenseMatrix.fill(k, numFeatures)(0.1)
        gmmVars(*, ::) :*= (colRange :* colRange)

        val gmmWeights = DenseVector.fill(k, 1.0 / k).asDenseMatrix

        (gmmWeights, gmmMeans, gmmVars)
    }

    // set the lower bound for the gmm_variance
    val gmmVarLB = max(smallVarianceThreshold * varianceGlobal, absoluteVarianceThreshold)

    // Threshold small variances
    gmmVars = max(gmmVars, DenseMatrix.ones[Double](k, 1) * gmmVarLB)

    // Run EM!
    val curCost = DenseVector.zeros[Double](maxIterations)
    var iter = 0
    var costImproving = true
    var largeEnoughClusters = true
    while ((iter < maxIterations) && costImproving && largeEnoughClusters) {
      /* E-STEP */

      // compute the squared malhanobis distance for each gaussian.
      // sq_mal_dist(i,j) || x_i - mu_j||_Lambda^2.
      val sqMahlist = (XSq * (0.5 :/ gmmVars).t) - (X * (gmmMeans :/ gmmVars).t)
      sqMahlist(*, ::) += (sum(gmmMeans :* gmmMeans :/ gmmVars, Axis._1) :* 0.5)

      // compute the log likelihood of the approximate posterior
      val llh = DenseMatrix.ones[Double](numSamples, 1) *
          (-0.5 * numFeatures * math.log(2 * math.Pi) - 0.5 * sum(bLog(gmmVars), Axis._1).t + bLog(gmmWeights)) -
          sqMahlist

      // compute the log likelihood of the model using the incremental
      // approach suggested by the Xerox folks.  The key thing here is that
      // for all intents and purposes, log(1+exp(t)) is equal to zero is t<-30
      // and equal to t if t>30
      val lseLLH = llh(::, 0).copy
      var cluster = 1
      while (cluster < k) {
        var sample = 0
        // Extract this cluster's llh as a vector
        val llhCluster = llh(::, cluster)
        while (sample < numSamples) {
          val deltaLSE = lseLLH(sample) - llhCluster(sample)
          var lseIncrement = 0.0
          if (deltaLSE > 30.0) {
            lseIncrement = deltaLSE
          } else if (deltaLSE > -30.0) {
            val deltaLSEThreshold = min(max(deltaLSE, -30.0), 30.0)
            lseIncrement = bLog(exp(deltaLSEThreshold) + 1.0)
          } // else t < -30 which adds no weight
          lseLLH(sample) = lseIncrement + llhCluster(sample)
          sample = sample + 1
        }
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

sealed trait GMMInitializationMethod extends Serializable
case object KMEANS_PLUS_PLUS_INITIALIZATION extends GMMInitializationMethod

/**
 * Randomly distributes the initial means within the minimum and maximum values
 * seen in the training data, using a uniform distribution.
 */
case object RANDOM_INITIALIZATION extends GMMInitializationMethod
