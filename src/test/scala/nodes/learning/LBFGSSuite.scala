package nodes.learning

import breeze.linalg._
import edu.berkeley.cs.amplab.mlmatrix.RowPartitionedMatrix
import org.apache.spark.SparkContext
import org.scalatest.FunSuite
import pipelines.{LocalSparkContext, Logging}
import utils.{MatrixUtils, Stats}

class LBFGSSuite extends FunSuite with LocalSparkContext with Logging {
  test("Solve a linear system") {
    sc = new SparkContext("local", "test")

    // Create the data.
    val A = RowPartitionedMatrix.createRandom(sc, 128, 5, 4, cache=true)
    val x = DenseVector(5.0, 4.0, 3.0, 2.0, -1.0).toDenseMatrix
    val b = A.mapPartitions(part => part * x.t)

    val Aary = A.rdd.flatMap(part => MatrixUtils.matrixToRowArray(part.mat).toIterator)
    val bary = b.rdd.flatMap(part => MatrixUtils.matrixToRowArray(part.mat).toIterator)

    val mapper = new LBFGSwithL2(new LeastSquaresBatchGradient()).fit(Aary, bary)

    assert(Stats.aboutEq(mapper.x, x.t, 1e-6), "Coefficients from the solve must match the hand-created model.")
  }
}
