package nodes.learning

import breeze.linalg._
import breeze.stats.distributions.{ThreadLocalRandomGenerator, RandBasis, Multinomial}
import breeze.stats.mean
import org.apache.commons.math3.random.MersenneTwister
import org.apache.spark.rdd.RDD
import pipelines.Logging
import workflow.{Estimator, Transformer}
import utils.MatrixUtils

/**
 * A KMeans assigning transformer
 * @param means matrix of dimension numClusters by numFeatures
 */
case class KMeansModel(means: DenseMatrix[Double]) extends Transformer[DenseVector[Double], DenseVector[Double]] {
  /**
   * Returns the assignment of each vector to the nearest cluster.
   */
  def apply(in: DenseVector[Double]): DenseVector[Double] = {
    // TODO: Could maybe do more efficient single-item implementation
    apply(in.asDenseMatrix).flatten()
  }

  def apply(in: DenseMatrix[Double]): DenseMatrix[Double] = {
    val XSqNormHlf: DenseVector[Double] = (sum(in :* in, Axis._1) :*= 0.5)
    /* compute the distance to all of the centers and assign each point to its nearest center. */
    val sqDistToCenters = in * means.t
    sqDistToCenters :*= -1.0
    sqDistToCenters(::, *) += XSqNormHlf
    sqDistToCenters(*, ::) += (sum(means :* means, Axis._1) :*= 0.5)

    /*
    sqDistToCenters is numExamples by numCenters. This argmin uses Breeze broadcasting to find
    the column index with the smallest value for each row (aka the nearest center for that example).
    nearestCenter is a vector of size numExamples.
    */
    val nearestCenter = argmin(sqDistToCenters(*, ::))

    /*
    Now we construct a center assignments matrix.
    It isa binary numExample by numCenters matrix that has the value 1.0 at a cell
    if that center (column) is the closest center to that example (row), and 0.0 if it is not.
    We reuse the previous (potentially large) matrix to minimize memory allocation.
    */
    val centerAssign = sqDistToCenters
    var row: Int = 0
    while (row < in.rows) {
      var col: Int = 0
      while (col < means.rows) {
        centerAssign(row, col) = 0.0
        col += 1
      }

      centerAssign(row, nearestCenter(row)) = 1.0
      row += 1
    }

    centerAssign
  }

  override def apply(in: RDD[DenseVector[Double]]): RDD[DenseVector[Double]] = {
    in.mapPartitions { partition =>
      MatrixUtils.rowsToMatrixIter(partition).flatMap { mat =>
        val assignments = apply(mat)
        MatrixUtils.matrixToRowArray(assignments).iterator
      }
    }
  }
}

/**
 * Trains a k-means++ transformer
 *
 * if you run for one round, this is the same as the k-means++
 * initialization.  If you run for more rounds, you are running Lloyd's
 * algorithm with the k-means++ initialization scheme.
 *
 * @param numMeans
 * @param maxIterations
 * @param stopTolerance Tolerance used to decide when to terminate Lloyd's algorithm
 */
case class KMeansPlusPlusEstimator(
    numMeans: Int,
    maxIterations: Int,
    stopTolerance: Double = 1e-3,
    seed: Int = 0)
    extends Estimator[DenseVector[Double], DenseVector[Double]] with Logging {
  def fit(data: RDD[DenseVector[Double]]): KMeansModel = {
    val X = MatrixUtils.rowsToMatrix(data.collect())
    fit(X)
  }

  def fit(X: DenseMatrix[Double]): KMeansModel = {
    val numSamples = X.rows
    val numFeatures = X.cols

    val XSqNormHlf: DenseVector[Double] = sum(X :* X, Axis._1) / 2.0

    val centers = Array.fill(numMeans)(0)
    // Not happy about marking this implicit, but Breeze Multinomial only takes a RandBasis as an implicit w/ a
    // whole bunch of other implicit things
    implicit val implicitRand = new RandBasis(new ThreadLocalRandomGenerator(new MersenneTwister(seed)))
    centers(0) = Multinomial(DenseVector.fill(numSamples, 1.0/numSamples)).draw()

    var curSqDistanceToClusters: DenseVector[Double] = null
    var k = 0
    while (k < (numMeans - 1)) {
      val curCenter = X(centers(k), ::)
      val curCenterNorm = norm(curCenter, 2)
      // slick vectorized code to compute the distance to the current center
      val sqDistToNewCenter = (XSqNormHlf - (X * curCenter.t)) += (0.5 * curCenterNorm * curCenterNorm)

      curSqDistanceToClusters = if (k > 0) {
        min(sqDistToNewCenter, curSqDistanceToClusters)
      } else {
        sqDistToNewCenter
      }

      // add a new center by the k-means++ rule
      centers(k + 1) = Multinomial(max(curSqDistanceToClusters, 0.0)).draw()

      k += 1
    }

    var kMeans = X(centers.toSeq, ::).toDenseMatrix
    val curCost = DenseVector.zeros[Double](maxIterations)
    var iter = 0
    var costImproving = true
    while ((iter < maxIterations) && costImproving) {
      /* compute the distance to all of the centers and assign each point to its
         nearest center. (Again, mad slick and vectorized). */
      val sqDistToCenters = X * kMeans.t
      sqDistToCenters :*= -1.0
      sqDistToCenters(::, *) += XSqNormHlf
      sqDistToCenters(*, ::) += (sum(kMeans :* kMeans, Axis._1) :*= 0.5)

      val bestDist = min(sqDistToCenters(*, ::))
      curCost(iter) = mean(bestDist)

      /*
      sqDistToCenters is numExamples by numCenters. This argmin uses Breeze broadcasting to find
      the column index with the smallest value for each row (aka the nearest center for that example).
      nearestCenter is a vector of size numExamples.
      */
      val nearestCenter = argmin(sqDistToCenters(*, ::))

      /*
      Now we construct a center assignments matrix.
      It isa binary numExample by numCenters matrix that has the value 1.0 at a cell
      if that center (column) is the closest center to that example (row), and 0.0 if it is not.
      We reuse the previous (potentially large) matrix to minimize memory allocation.
      */
      val centerAssign = sqDistToCenters
      var row: Int = 0
      while (row < numSamples) {
        var col: Int = 0
        while (col < numMeans) {
          centerAssign(row, col) = 0.0
          col += 1
        }

        centerAssign(row, nearestCenter(row)) = 1.0
        row += 1
      }

      val assignMass = sum(centerAssign, Axis._0).t
      kMeans = centerAssign.t * X
      kMeans(::, *) :/= assignMass

      if (iter > 0) {
        costImproving = (curCost(iter - 1) - curCost(iter)) >= stopTolerance * math.abs(curCost(iter - 1))
        logInfo("Iteration: " + iter + " current cost " + curCost(iter) + " imp " + costImproving)
      }

      iter += 1
    }

    KMeansModel(kMeans)
  }
}
