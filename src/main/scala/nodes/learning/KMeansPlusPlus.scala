package nodes.learning

import breeze.linalg._
import breeze.stats.distributions.Multinomial
import breeze.stats.mean
import org.apache.spark.rdd.RDD
import pipelines.{Estimator, Transformer}
import utils.MatrixUtils

case class KMeansModel(means: DenseMatrix[Double]) extends Transformer[DenseVector[Double], DenseVector[Double]] {
  /**
   * For now this is unimplemented. It should return the hard assignment to each cluster.
   * @param in A Vector
   * @return The assignment of the vector according to the kmeans model.
   */
  def apply(in: DenseVector[Double]): DenseVector[Double] = ???
}

/**
 * if you run for one round, this is the same as the k-means++
 * initialization.  If you run for more rounds, you are running Lloyd's
 * algorithm with the k-means++ initialization scheme.
 *
 * @param numMeans
 * @param numRounds
 * @param stopTolerance Tolerance used to decide when to terminate Lloyd's algorithm
 */
case class KMeansPlusPlusEstimator(numMeans: Int, numRounds: Int, stopTolerance: Double = 1e-3) extends Estimator[DenseVector[Double], DenseVector[Double]] {
  def fit(data: RDD[DenseVector[Double]]): KMeansModel = {
    fit(data.collect())
  }

  def fit(samples: Array[DenseVector[Double]]): KMeansModel = {
    val X = MatrixUtils.rowsToMatrix(samples)
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
      val sqDistToNewCenter = XSqNormHlf - (X * curCenter.t) + (0.5 * curCenterNorm * curCenterNorm)

      curSqDistanceToClusters = if (k > 0) {
        DenseVector((0 until numSamples).map(i => math.min(sqDistToNewCenter(i), curSqDistanceToClusters(i))).toArray)
      } else {
        sqDistToNewCenter
      }

      // add a new center by the k-means++ rule
      centers(k + 1) = Multinomial(curSqDistanceToClusters.map(math.max(0, _))).draw()

      k += 1
    }

    var kMeans = X(centers.toSeq, ::).toDenseMatrix
    val curCost = DenseVector.zeros[Double](numRounds)
    var iter = 0
    var costImproving = true
    while ((iter < numRounds) && costImproving) {
      /* compute the distance to all of the centers and assign each point to its
         nearest center. (Again, mad slick and vectorized). */
      val sqDistToCenters = (XSqNormHlf * DenseMatrix.ones[Double](1, numMeans)) - (X * kMeans.t) + (DenseMatrix.ones[Double](numSamples, 1) * (0.5 * sum(kMeans :* kMeans, Axis._1)).t)
      val bestDist = min(sqDistToCenters(*, ::))
      curCost(iter) = mean(bestDist)

      val nearestCenter = argmin(sqDistToCenters(*, ::))

      val centerAssign = DenseMatrix.tabulate(numSamples, numMeans) {
        case (row: Int, col: Int) => {
          if (nearestCenter(row) == col) 1.0 else 0.0
        }
      }

      val assignMass = sum(centerAssign, Axis._0)
      kMeans = diag(assignMass.map(1.0 / _)) * (centerAssign.t * X)

      iter += 1
      if (iter > 0) {
        costImproving = (curCost(iter - 1) - curCost(iter)) < stopTolerance * math.abs(curCost(iter - 1))
      }
    }

    KMeansModel(kMeans)
  }
}
