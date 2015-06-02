package nodes.learning

import breeze.linalg._
import org.apache.spark.SparkContext
import org.scalatest.FunSuite
import pipelines._
import utils.{MatrixUtils, Stats}

class BensGMMSuite extends FunSuite with LocalSparkContext with Logging {

  test("GMM Single Center") {
    sc = new SparkContext("local", "test")

    val k = 1

    val data = sc.parallelize(Array(
      DenseVector[Double](1.0, 2.0, 6.0),
      DenseVector[Double](1.0, 3.0, 0.0),
      DenseVector[Double](1.0, 4.0, 6.0)
    ))

    val center = DenseVector[Double](1.0, 3.0, 4.0).asDenseMatrix
    val gmm = BensGMMEstimator(k, minClusterSize = 1).fit(data)

    assert(gmm.means.t === center)

    // TODO: TEST Variances!
    gmm.apply(data)
  }

  test("GMM Two Centers") {
    sc = new SparkContext("local", "test")

    val k = 2

    val data = sc.parallelize(Array(
      DenseVector[Double](1.0, 2.0, 6.0),
      DenseVector[Double](1.0, 3.0, 0.0),
      DenseVector[Double](1.0, 4.0, 6.0),
      DenseVector[Double](1.0, 1.0, 0.0)
    ))

    val centers = Set(
      DenseVector[Double](1.0, 2.0, 0.0),
      DenseVector[Double](1.0, 3.0, 6.0)
    )

    val gmm = BensGMMEstimator(k, minClusterSize = 1).fit(data)
    // TODO: TEST Means & Variances!

    /*val fitCenters = MatrixUtils.matrixToRowArray(kMeans.means).toSet
    assert(fitCenters === centers )

    val kMeans5 = KMeansPlusPlusEstimator(k, maxIterations = 5).fit(data)
    val fitCenters5 = MatrixUtils.matrixToRowArray(kMeans5.means).toSet
    assert(fitCenters5 === centers )

    val out = kMeans.apply(data).collect()*/
    logInfo("sup")
  }

  test("GaussianMixtureModel test") {
    sc = new SparkContext("local", "test")

    val data = Array(
      DenseVector[Double](1.0, 2.0, 6.0),
      DenseVector[Double](1.0, 3.0, 0.0),
      DenseVector[Double](1.0, 4.0, 6.0),
      DenseVector[Double](1.0, 1.0, 0.0)
    )

    val means = MatrixUtils.rowsToMatrix(Array(
      DenseVector[Double](1.0, 2.0, 0.0),
      DenseVector[Double](1.0, 3.0, 6.0)
    )).t

    val variances = MatrixUtils.rowsToMatrix(Array(
      DenseVector[Double](1e-8, 1.0, 0.09),
      DenseVector[Double](1e-8, 1.0, 0.09)
    )).t

    val weights = DenseVector[Double](0.5, 0.5)

    val clusterOne = DenseVector[Double](1.0, 0.0)
    val clusterTwo = DenseVector[Double](0.0, 1.0)

    val assignments = Seq(clusterTwo, clusterOne, clusterTwo, clusterOne)
    val gmm = GaussianMixtureModel(means, variances, weights)

    // Test Single Apply
    assert(gmm.apply(DenseVector[Double](1.0, 3.0, 0.0)) === clusterOne)
    assert(gmm.apply(DenseVector[Double](1.0, 1.0, 0.0)) === clusterOne)
    assert(gmm.apply(DenseVector[Double](1.0, 2.0, 6.0)) === clusterTwo)
    assert(gmm.apply(DenseVector[Double](1.0, 4.0, 6.0)) === clusterTwo)

    // Test Matrix Apply
    assert(gmm.apply(MatrixUtils.rowsToMatrix(data)) === MatrixUtils.rowsToMatrix(assignments))

    // Test RDD Apply
    assert(gmm.apply(sc.parallelize(data)).collect().toSeq === assignments)
  }
}
