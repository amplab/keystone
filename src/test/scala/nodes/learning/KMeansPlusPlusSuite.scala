package nodes.learning

import breeze.linalg._
import org.apache.spark.SparkContext
import org.scalatest.FunSuite
import pipelines._
import utils.{MatrixUtils, Stats}
import workflow.PipelineContext

class KMeansPlusPlusSuite extends FunSuite with PipelineContext with Logging {

  test("K-Means++ Single Center") {
    sc = new SparkContext("local", "test")

    val k = 1

    val data = sc.parallelize(Array(
      DenseVector[Double](1.0, 2.0, 6.0),
      DenseVector[Double](1.0, 3.0, 0.0),
      DenseVector[Double](1.0, 4.0, 6.0)
    ))

    val center = DenseVector[Double](1.0, 3.0, 4.0).asDenseMatrix

    val kMeans = KMeansPlusPlusEstimator(k, maxIterations = 1).fit(data)
    assert(Stats.aboutEq(kMeans.means, center))

    val kMeans10 = KMeansPlusPlusEstimator(k, maxIterations = 10).fit(data)
    assert(Stats.aboutEq(kMeans.means, center))

    val out = kMeans.apply(data).collect()
  }

  test("K-Means++ Two Centers") {
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

    val kMeans = KMeansPlusPlusEstimator(k, maxIterations = 10).fit(data)
    val fitCenters = MatrixUtils.matrixToRowArray(kMeans.means).toSet
    assert(fitCenters === centers )

    val kMeans5 = KMeansPlusPlusEstimator(k, maxIterations = 5).fit(data)
    val fitCenters5 = MatrixUtils.matrixToRowArray(kMeans5.means).toSet
    assert(fitCenters5 === centers )

    val out = kMeans.apply(data).collect()
  }

  test("K-Means Transformer") {
    sc = new SparkContext("local", "test")

    val data = Array(
      DenseVector[Double](1.0, 2.0, 6.0),
      DenseVector[Double](1.0, 3.0, 0.0),
      DenseVector[Double](1.0, 4.0, 6.0),
      DenseVector[Double](1.0, 1.0, 0.0)
    )

    val centers = MatrixUtils.rowsToMatrix(Array(
      DenseVector[Double](1.0, 2.0, 0.0),
      DenseVector[Double](1.0, 3.0, 6.0)
    ))

    val clusterOne = DenseVector[Double](1.0, 0.0)
    val clusterTwo = DenseVector[Double](0.0, 1.0)

    val assignments = Seq(clusterTwo, clusterOne, clusterTwo, clusterOne)
    val kMeans = KMeansModel(centers)

    // Test Single Apply
    assert(kMeans.apply(DenseVector[Double](1.0, 3.0, 0.0)) === clusterOne)
    assert(kMeans.apply(DenseVector[Double](1.0, 1.0, 0.0)) === clusterOne)
    assert(kMeans.apply(DenseVector[Double](1.0, 2.0, 6.0)) === clusterTwo)
    assert(kMeans.apply(DenseVector[Double](1.0, 4.0, 6.0)) === clusterTwo)

    // Test Matrix Apply
    assert(kMeans.apply(MatrixUtils.rowsToMatrix(data)) === MatrixUtils.rowsToMatrix(assignments))

    // Test RDD Apply
    assert(kMeans.apply(sc.parallelize(data)).collect().toSeq === assignments)
  }
}
