package nodes.learning

import breeze.linalg._
import edu.berkeley.cs.amplab.mlmatrix.RowPartitionedMatrix
import org.apache.spark.SparkContext
import org.scalatest.FunSuite
import pipelines.{LocalSparkContext, Logging}
import utils.{MatrixUtils, Stats}

class LinearMapperSuite extends FunSuite with LocalSparkContext with Logging {
  test("Solve and apply a linear system") {
    sc = new SparkContext("local", "test")

    // Create the data.
    val A = RowPartitionedMatrix.createRandom(sc, 128, 5, 4, cache=true)
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
    val A = RowPartitionedMatrix.createRandom(sc, 50, 400, 4, cache=true)
    val x = DenseVector(5.0, 4.0, 3.0, 2.0, -1.0).toDenseMatrix
    val b = A.mapPartitions(part => DenseMatrix.rand(part.rows, 3))

    val Aary = A.rdd.flatMap(part => MatrixUtils.matrixToRowArray(part.mat).toIterator)
    val bary = b.rdd.flatMap(part => MatrixUtils.matrixToRowArray(part.mat).toIterator)

    val mapper = new LocalLeastSquaresEstimator(1e-2).fit(Aary, bary)
    assert(mapper.x.rows === 400)
    assert(mapper.x.cols === 3)
  }
}
