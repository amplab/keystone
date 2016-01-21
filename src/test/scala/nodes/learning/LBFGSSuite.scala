package nodes.learning

import breeze.linalg._
import edu.berkeley.cs.amplab.mlmatrix.RowPartitionedMatrix
import org.apache.spark.SparkContext
import org.scalatest.FunSuite
import pipelines.{LocalSparkContext, Logging}
import utils.{MatrixUtils, Stats}

class LBFGSSuite extends FunSuite with LocalSparkContext with Logging {
  test("Solve a dense linear system (fit intercept)") {
    sc = new SparkContext("local", "test")

    // Create the data.
    val A = RowPartitionedMatrix.createRandom(sc, 128, 5, 4, cache=true)
    val x = DenseMatrix((5.0, 4.0, 3.0, 2.0, -1.0), (3.0, -1.0, 2.0, -2.0, 1.0))
    val b = A.mapPartitions(part => part * x.t)

    val Aary = A.rdd.flatMap(part => MatrixUtils.matrixToRowArray(part.mat).toIterator)
      .map(_ + DenseVector(1.0, 0.0, 1.0, 2.0, 0.0))
    val bary = b.rdd.flatMap(part => MatrixUtils.matrixToRowArray(part.mat).toIterator).map(_ + DenseVector(3.0, 4.0))

    val mapper = new DenseLBFGSwithL2(new LeastSquaresDenseGradient(), fitIntercept = true).fit(Aary, bary)

    val trueResult = MatrixUtils.rowsToMatrix(bary.collect())
    val solverResult = MatrixUtils.rowsToMatrix(mapper(Aary).collect())

    assert(Stats.aboutEq(trueResult, solverResult, 1e-5), "Results from the solve must match the hand-created model.")
  }

  test("Solve a dense linear system (no fit intercept)") {
    sc = new SparkContext("local", "test")

    // Create the data.
    val A = RowPartitionedMatrix.createRandom(sc, 128, 5, 4, cache=true)
    val x = DenseMatrix((5.0, 4.0, 3.0, 2.0, -1.0), (3.0, -1.0, 2.0, -2.0, 1.0))
    val b = A.mapPartitions(part => part * x.t)

    val Aary = A.rdd.flatMap(part => MatrixUtils.matrixToRowArray(part.mat).toIterator)
    val bary = b.rdd.flatMap(part => MatrixUtils.matrixToRowArray(part.mat).toIterator)

    val mapper = new DenseLBFGSwithL2(new LeastSquaresDenseGradient(), fitIntercept = false).fit(Aary, bary)

    val trueResult = MatrixUtils.rowsToMatrix(bary.collect())
    val solverResult = MatrixUtils.rowsToMatrix(mapper(Aary).collect())

    assert(Stats.aboutEq(trueResult, solverResult, 1e-5), "Results from the solve must match the hand-created model.")
  }

  test("Solve a sparse linear system (fit intercept)") {
    sc = new SparkContext("local", "test")

    // Create the data.
    val A = RowPartitionedMatrix.createRandom(sc, 128, 5, 4, cache=true)
    val x = DenseMatrix((5.0, 4.0, 3.0, 2.0, -1.0), (3.0, -1.0, 2.0, -2.0, 1.0))
    val b = A.mapPartitions(part => part * x.t)

    val Aary = A.rdd.flatMap(part => MatrixUtils.matrixToRowArray(part.mat).toIterator)
      .map(x => SparseVector((x + DenseVector(1.0, 0.0, 1.0, 2.0, 0.0)).toArray))
    val bary = b.rdd.flatMap(part => MatrixUtils.matrixToRowArray(part.mat).toIterator).map(_ + DenseVector(3.0, 4.0))

    val mapper = new SparseLBFGSwithL2(new LeastSquaresSparseGradient(), fitIntercept = true).fit(Aary, bary)

    val trueResult = MatrixUtils.rowsToMatrix(bary.collect())
    val solverResult = MatrixUtils.rowsToMatrix(mapper(Aary).collect())

    assert(Stats.aboutEq(trueResult, solverResult, 1e-4), "Results from the solve must match the hand-created model.")
  }

  test("Solve a sparse linear system (no fit intercept)") {
    sc = new SparkContext("local", "test")

    // Create the data.
    val A = RowPartitionedMatrix.createRandom(sc, 128, 5, 4, cache=true)
    val x = DenseMatrix((5.0, 4.0, 3.0, 2.0, -1.0), (3.0, -1.0, 2.0, -2.0, 1.0))
    val b = A.mapPartitions(part => part * x.t)

    val Aary = A.rdd.flatMap(part => MatrixUtils.matrixToRowArray(part.mat).toIterator)
      .map(x => SparseVector(x.toArray))
    val bary = b.rdd.flatMap(part => MatrixUtils.matrixToRowArray(part.mat).toIterator)

    val mapper = new SparseLBFGSwithL2(new LeastSquaresSparseGradient(), fitIntercept = false).fit(Aary, bary)

    val trueResult = MatrixUtils.rowsToMatrix(bary.collect())
    val solverResult = MatrixUtils.rowsToMatrix(mapper(Aary).collect())

    assert(Stats.aboutEq(trueResult, solverResult, 1e-5), "Results from the solve must match the hand-created model.")
  }
}
