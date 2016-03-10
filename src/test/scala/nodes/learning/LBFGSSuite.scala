package nodes.learning

import breeze.linalg._
import edu.berkeley.cs.amplab.mlmatrix.RowPartitionedMatrix
import nodes.stats.StandardScaler
import org.apache.spark.{rdd, SparkContext}
import org.scalatest.FunSuite
import pipelines.{LocalSparkContext, Logging}
import utils.{TestUtils, MatrixUtils, Stats}

class LBFGSSuite extends FunSuite with LocalSparkContext with Logging {
  test("Solve a dense linear system (fit intercept)") {
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

    val mapper = new DenseLBFGSwithL2(new LeastSquaresDenseGradient(), fitIntercept = true).fit(Aary, bary)

    val trueResult = MatrixUtils.rowsToMatrix(bary.collect())
    val solverResult = MatrixUtils.rowsToMatrix(mapper(Aary).collect())

    assert(Stats.aboutEq(trueResult, solverResult, 1e-5), "Results from the solve must match the hand-created model.")
    assert(Stats.aboutEq(mapper.x, x.t, 1e-5), "Model weights from the solve must match the hand-created model.")
    assert(Stats.aboutEq(mapper.bOpt.get, extraBias, 1e-5), "Learned intercept must match the hand-created model.")
    assert(Stats.aboutEq(mapper.featureScaler.get.mean, dataMean, 1e-5),
      "Learned intercept must match the hand-created model.")

  }

  test("Solve a dense linear system (no fit intercept)") {
    sc = new SparkContext("local", "test")

    // Create the data.
    val A = TestUtils.createRandomMatrix(sc, 128, 5, 4)
    val x = DenseMatrix((5.0, 4.0, 3.0, 2.0, -1.0), (3.0, -1.0, 2.0, -2.0, 1.0))
    val b = A.mapPartitions(part => part * x.t)

    val Aary = A.rdd.flatMap(part => MatrixUtils.matrixToRowArray(part.mat).toIterator)
    val bary = b.rdd.flatMap(part => MatrixUtils.matrixToRowArray(part.mat).toIterator)

    val mapper = new DenseLBFGSwithL2(new LeastSquaresDenseGradient(), fitIntercept = false).fit(Aary, bary)

    val trueResult = MatrixUtils.rowsToMatrix(bary.collect())
    val solverResult = MatrixUtils.rowsToMatrix(mapper(Aary).collect())

    assert(Stats.aboutEq(trueResult, solverResult, 1e-5), "Results from the solve must match the hand-created model.")
    assert(Stats.aboutEq(mapper.x, x.t, 1e-5), "Model weights from the solve must match the hand-created model.")
    assert(mapper.bOpt.isEmpty, "Not supposed to have learned an intercept.")
    assert(mapper.featureScaler.isEmpty, "Not supposed to have learned an intercept.")
  }

  test("Solve a sparse linear system (fit intercept)") {
    sc = new SparkContext("local", "test")

    // Create the data.
    val A = TestUtils.createRandomMatrix(sc, 128, 5, 4)
    val x = DenseMatrix((5.0, 4.0, 3.0, 2.0, -1.0), (3.0, -1.0, 2.0, -2.0, 1.0))
    val dataMean = DenseVector(1.0, 0.0, 1.0, 2.0, 0.0)
    val extraBias = DenseVector(3.0, 4.0)
    val b = A.mapPartitions(part => part * x.t)

    val Aary = A.rdd.flatMap(part => MatrixUtils.matrixToRowArray(part.mat).toIterator)
      .map(x => SparseVector((x + dataMean).toArray))
    val bary = b.rdd.flatMap(part => MatrixUtils.matrixToRowArray(part.mat).toIterator).map(_ + extraBias)

    val mapper = new SparseLBFGSwithL2(new LeastSquaresSparseGradient(), fitIntercept = true).fit(Aary, bary)

    val trueResult = MatrixUtils.rowsToMatrix(bary.collect())
    val solverResult = MatrixUtils.rowsToMatrix(mapper(Aary).collect())

    assert(Stats.aboutEq(trueResult, solverResult, 1e-3), "Results from the solve must match the hand-created model.")
    assert(Stats.aboutEq(mapper.x, x.t, 1e-3), "Model weights from the solve must match the hand-created model.")

    val trueBias = extraBias - (x * dataMean)
    assert(Stats.aboutEq(mapper.bOpt.get, trueBias, 1e-3), "Learned intercept must match the hand-created model.")
  }

  test("Solve a sparse linear system (no fit intercept)") {
    sc = new SparkContext("local", "test")

    // Create the data.
    val A = TestUtils.createRandomMatrix(sc, 128, 5, 4)
    val x = DenseMatrix((5.0, 4.0, 3.0, 2.0, -1.0), (3.0, -1.0, 2.0, -2.0, 1.0))
    val b = A.mapPartitions(part => part * x.t)

    val Aary = A.rdd.flatMap(part => MatrixUtils.matrixToRowArray(part.mat).toIterator)
      .map(x => SparseVector(x.toArray))
    val bary = b.rdd.flatMap(part => MatrixUtils.matrixToRowArray(part.mat).toIterator)

    val mapper = new SparseLBFGSwithL2(new LeastSquaresSparseGradient(), fitIntercept = false).fit(Aary, bary)

    val trueResult = MatrixUtils.rowsToMatrix(bary.collect())
    val solverResult = MatrixUtils.rowsToMatrix(mapper(Aary).collect())

    assert(Stats.aboutEq(trueResult, solverResult, 1e-4), "Results from the solve must match the hand-created model.")
    assert(Stats.aboutEq(mapper.x, x.t, 1e-4), "Model weights from the solve must match the hand-created model.")
    assert(mapper.bOpt.isEmpty, "Not supposed to have learned an intercept.")
  }
}
