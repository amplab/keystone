package keystoneml.nodes.learning

import breeze.linalg._
import org.apache.spark.SparkContext
import org.scalatest.FunSuite
import keystoneml.pipelines._
import keystoneml.utils.{TestUtils, MatrixUtils, Stats}
import keystoneml.workflow.PipelineContext

class GaussianMixtureModelSuite extends FunSuite with PipelineContext with Logging {

  test("GMM Single Center") {
    sc = new SparkContext("local", "test")

    val k = 1

    val data = sc.parallelize(Array(
      DenseVector[Double](1.0, 2.0, 6.0),
      DenseVector[Double](1.0, 3.0, 0.0),
      DenseVector[Double](1.0, 4.0, 6.0)
    ))

    val center = DenseVector[Double](1.0, 3.0, 4.0).asDenseMatrix
    val gmm = GaussianMixtureModelEstimator(k, minClusterSize = 1, seed = 0).fit(data)

    assert(gmm.means.t === center)

    gmm.apply(data)
  }

  test("GMM Two Centers dataset 1") {
    sc = new SparkContext("local", "test")
    val k = 2

    val gmmEst = GaussianMixtureModelEstimator(k, minClusterSize = 1, seed = 0)


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

    val variances = Set(
      DenseVector[Double](max(gmmEst.absoluteVarianceThreshold, 0.0), 1.0, 0.09),
      DenseVector[Double](max(gmmEst.absoluteVarianceThreshold, 0.0), 1.0, 0.09)
    )

    val gmm = gmmEst.fit(data)

    val fitCenters = MatrixUtils.matrixToRowArray(gmm.means.t).toSet
    assert(fitCenters === centers )

    val fitVariances = MatrixUtils.matrixToRowArray(gmm.variances.t).toSet
    assert(fitVariances === variances )
  }

  test("GMM Two Centers dataset 2") {
    sc = new SparkContext("local", "test")
    val k = 2

    val gmmEst = GaussianMixtureModelEstimator(k, minClusterSize = 1, seed = 0)


    // Data, centers, variances from the Spark MLlib gaussian mixture suite
    val data = sc.parallelize(Array(
      DenseVector(-5.1971), DenseVector(-2.5359), DenseVector(-3.8220),
      DenseVector(-5.2211), DenseVector(-5.0602), DenseVector( 4.7118),
      DenseVector( 6.8989), DenseVector( 3.4592), DenseVector( 4.6322),
      DenseVector( 5.7048), DenseVector( 4.6567), DenseVector( 5.5026),
      DenseVector( 4.5605), DenseVector( 5.2043), DenseVector( 6.2734)
    ))

    val centersOrder1 = new DenseMatrix[Double](1, 2, Array(5.1604, -4.3673))
    val centersOrder2 = new DenseMatrix[Double](1, 2, Array(-4.3673, 5.1604))

    val variancesOrder1 = new DenseMatrix[Double](1, 2, Array(0.86644, 1.1098))
    val variancesOrder2 = new DenseMatrix[Double](1, 2, Array(1.1098, 0.86644))

    val gmm = gmmEst.fit(data)

    val inOrder1 = Stats.aboutEq(centersOrder1, gmm.means, 1e-4) && Stats.aboutEq(variancesOrder1, gmm.variances, 1e-4)
    val inOrder2 = Stats.aboutEq(centersOrder2, gmm.means, 1e-4) && Stats.aboutEq(variancesOrder2, gmm.variances, 1e-4)

    assert(inOrder1 || inOrder2)
  }

  test("GMM Two Centers dataset 3") {
    sc = new SparkContext("local", "test")
    val k = 2

    val gmmEst = GaussianMixtureModelEstimator(k, minClusterSize = 1, seed = 0, stopTolerance = 0, maxIterations = 30)

    val data = sc.parallelize(TestUtils.loadFile("gmm_data.txt"))
    val parsedData = data.map(s => DenseVector[Double](s.trim.split(' ').map(_.toDouble))).cache()


    val centers = DenseMatrix.zeros[Double](2, 2)

    val variances = DenseMatrix((1.0, 25.0), (25.0, 1.0))

    val weights = DenseVector(0.5, 0.5)

    val gmm = gmmEst.fit(parsedData)

    assert(Stats.aboutEq(centers, gmm.means.t, 0.5))

    assert(Stats.aboutEq(variances, gmm.variances.t, 2.0))

    assert(Stats.aboutEq(gmm.weights, weights, 0.05))
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
