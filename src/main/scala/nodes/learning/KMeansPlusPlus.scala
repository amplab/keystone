package nodes.learning

import breeze.linalg._
import breeze.stats.distributions.Multinomial
import breeze.stats.mean
import org.apache.spark.rdd.RDD
import pipelines.{Estimator, Transformer}
import utils.MatrixUtils

/**
 *
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
    val XSqNormHlf: DenseVector[Double] = sum(in :* in, Axis._1) / 2.0
    /* compute the distance to all of the centers and assign each point to its nearest center. */
    val sqDistToCenters = in * means.t
    sqDistToCenters :*= -1.0
    sqDistToCenters(::, *) += XSqNormHlf
    sqDistToCenters(*, ::) += (sum(means :* means, Axis._1) :*= 0.5)

    val nearestCenter = argmin(sqDistToCenters(*, ::))

    // reuse the previous (potentially large) matrix to keep memory usage down
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
      val assignments = apply(MatrixUtils.rowsToMatrix(partition))
      MatrixUtils.matrixToRowArray(assignments).iterator
    }
  }
}

/**
 * if you run for one round, this is the same as the k-means++
 * initialization.  If you run for more rounds, you are running Lloyd's
 * algorithm with the k-means++ initialization scheme.
 *
 * @param numMeans
 * @param maxIterations
 * @param stopTolerance Tolerance used to decide when to terminate Lloyd's algorithm
 */
case class KMeansPlusPlusEstimator(numMeans: Int, maxIterations: Int, stopTolerance: Double = 1e-3) extends Estimator[DenseVector[Double], DenseVector[Double]] {
  def fit(data: RDD[DenseVector[Double]]): KMeansModel = {
    val X = MatrixUtils.rowsToMatrix(data.collect())
    fit(X)
  }

  def fit(X: DenseMatrix[Double]): KMeansModel = {
    val numSamples = X.rows
    val numFeatures = X.cols

    val XSqNormHlf: DenseVector[Double] = sum(X :* X, Axis._1) / 2.0

    val centers = Array.fill(numMeans)(0)
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

      val nearestCenter = argmin(sqDistToCenters(*, ::))

      // For memory efficiency reuse the big sqDistToCenters matrix
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

      val assignMass = sum(centerAssign, Axis._0).toDenseVector
      kMeans = centerAssign.t * X
      kMeans(::, *) :/= assignMass

      if (iter > 0) {
        costImproving = (curCost(iter - 1) - curCost(iter)) >= stopTolerance * math.abs(curCost(iter - 1))
      }

      iter += 1
    }

    KMeansModel(kMeans)
  }
}
