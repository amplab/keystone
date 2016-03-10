package nodes.learning

import breeze.linalg._
import edu.berkeley.cs.amplab.mlmatrix.RowPartitionedMatrix
import nodes.stats.StandardScaler
import org.apache.spark.SparkContext
import org.scalatest.FunSuite
import pipelines.{LocalSparkContext, Logging}
import utils.{TestUtils, MatrixUtils, Stats}

class LinearMapperSuite extends FunSuite with LocalSparkContext with Logging {
  test("Solve and apply a linear system") {
    sc = new SparkContext("local", "test")

    // Create the data.
    val A = TestUtils.createRandomMatrix(sc, 128, 5, 4)
    val x = DenseVector(5.0, 4.0, 3.0, 2.0, -1.0).toDenseMatrix
    val b = A.mapPartitions(part => part * x.t)

    val Aary = A.rdd.flatMap(part => MatrixUtils.matrixToRowArray(part.mat).toIterator)
    val bary = b.rdd.flatMap(part => MatrixUtils.matrixToRowArray(part.mat).toIterator)

    val mapper = new LinearMapEstimator().fit(Aary, bary)

    assert(Stats.aboutEq(mapper.x, x.t), "Coefficients from the solve must match the hand-created model.")

    val point = DenseVector(2.0, -3.0, 2.0, 3.0, 5.0)

    assert(Stats.aboutEq(mapper(sc.parallelize(Seq(point))).first()(0), 5.0),
        "Linear model applied to a point should be 5.0")

    val bt = mapper(Aary)
    assert(Stats.aboutEq(bt.collect()(0), bary.collect()(0)),
        "Linear model applied to input should be the same as training points.")
  }

  test("LocalLeastSquaresEstimator doesn't crash") {
    sc = new SparkContext("local", "test")

    // Create the data.
    val A = TestUtils.createRandomMatrix(sc, 50, 400, 4)
    val x = DenseVector(5.0, 4.0, 3.0, 2.0, -1.0).toDenseMatrix
    val b = A.mapPartitions(part => DenseMatrix.rand(part.rows, 3))

    val Aary = A.rdd.flatMap(part => MatrixUtils.matrixToRowArray(part.mat).toIterator)
    val bary = b.rdd.flatMap(part => MatrixUtils.matrixToRowArray(part.mat).toIterator)

    val mapper = new LocalLeastSquaresEstimator(1e-2).fit(Aary, bary)
    assert(mapper.x.rows === 400)
    assert(mapper.x.cols === 3)
  }

  test("Solve a dense linear system (fit intercept) using local least squares") {
    sc = new SparkContext("local", "test")

    // Create the data.
    val A = TestUtils.createRandomMatrix(sc, 128, 5, 4)
    val x = DenseMatrix((5.0, 4.0, 3.0, 2.0, -1.0), (3.0, -1.0, 2.0, -2.0, 1.0))
    val dataMean = DenseVector(1.0, 0.0, 1.0, 2.0, 0.0)
    val extraBias = DenseVector(3.0, 4.0)

    val initialAary = A.rdd.flatMap(part => MatrixUtils.matrixToRowArray(part.mat).toIterator)
    val meanScaler = new StandardScaler(normalizeStdDev = false).fit(initialAary)
    val Aary = meanScaler.apply(initialAary).map(_ + dataMean)
    val bary = Aary.map(a => (x * (a - dataMean)) + extraBias)

    val mapper = new LocalLeastSquaresEstimator(0).fit(Aary, bary)

    val trueResult = MatrixUtils.rowsToMatrix(bary.collect())
    val solverResult = MatrixUtils.rowsToMatrix(mapper(Aary).collect())

    assert(Stats.aboutEq(trueResult, solverResult, 1e-5), "Results from the solve must match the hand-created model.")
    assert(Stats.aboutEq(mapper.x, x.t, 1e-6), "Model weights from the solve must match the hand-created model.")
    assert(Stats.aboutEq(mapper.bOpt.get, extraBias, 1e-6), "Learned intercept must match the hand-created model.")
    assert(Stats.aboutEq(mapper.featureScaler.get.mean, dataMean, 1e-6),
      "Learned intercept must match the hand-created model.")

  }

}
