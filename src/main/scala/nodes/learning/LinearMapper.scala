package nodes.learning

import breeze.linalg._
import edu.berkeley.cs.amplab.mlmatrix.{NormalEquations, RowPartitionedMatrix}
import org.apache.spark.rdd.RDD

import pipelines._
import utils.MatrixUtils

case class LinearMapper(x: DenseMatrix[Double])
  extends Transformer[Array[Double], Array[Double]]
  with Serializable {

  /**
   * Apply a linear model to an input.
   * @param in Input.
   * @return Output.
   */
  def transform(in: Array[Double]): Array[Double] = {
    (DenseMatrix(Array(in).toSeq:_*) * x).data
  }

  /**
   * Apply a linear model to a collection of inputs.
   *
   * Here we override the default behavior by first broadcasting the model before applying it.
   *
   * @param in Collection of A's.
   * @return Collection of B's.
   */
  override def transform(in: RDD[Array[Double]]): RDD[Array[Double]] = {
    val modelBroadcast = in.context.broadcast(x)
    in.mapPartitions(rows => {
      val mat = DenseMatrix(rows.toSeq:_*) * modelBroadcast.value
      MatrixUtils.matrixToRowArray(mat).iterator
    })
  }
}

object LinearMapper extends SupervisedEstimator[Array[Double], Array[Double], Array[Double]] {

  /**
   * Learns a linear model (OLS) based on training features and training labels.
   *
   * @param trainingFeatures Training features.
   * @param trainingLabels Training labels.
   * @return
   */
  def fit(
      trainingFeatures: RDD[Array[Double]],
      trainingLabels: RDD[Array[Double]]): LinearMapper = {
    val A = RowPartitionedMatrix.fromArray(trainingFeatures.map(_.map(_.toDouble)))
    val b = RowPartitionedMatrix.fromArray(trainingLabels.map(_.map(_.toDouble)))
    val x = new NormalEquations().solveLeastSquares(A, b)
    LinearMapper(x)
  }

  /**
   * Learns an L2-regularized linear model (OLS) based on training features and labels.
   *
   * @param trainingFeatures Training features.
   * @param trainingLabels Training labels.
   * @param lambda Regularization parameters. (>= 0.0)
   * @return
   */
  def fit(
      trainingFeatures: RDD[Array[Double]],
      trainingLabels: RDD[Array[Double]],
      lambda: Double): LinearMapper = {

    assert(lambda >= 0.0, "Regularization parameter must be at least 0.")

    val A = RowPartitionedMatrix.fromArray(trainingFeatures.map(_.map(_.toDouble)))
    val b = RowPartitionedMatrix.fromArray(trainingLabels.map(_.map(_.toDouble)))
    val x = new NormalEquations().solveLeastSquaresWithL2(A, b, lambda)
    LinearMapper(x)
  }

  /**
   * Learns several L2-regularized linear models (OLS) based on training features and labels.
   *
   * @param trainingFeatures Training features.
   * @param trainingLabels Training labels.
   * @param lambdas Regularization parameters (All >= 0.0).
   * @return
   */
  def fit(
      trainingFeatures: RDD[Array[Double]],
      trainingLabels: RDD[Array[Double]],
      lambdas: Array[Double]): Seq[LinearMapper] = {

    assert(lambdas.forall(_ >= 0.0), "All lambdas must be at least 0.")

    val A = RowPartitionedMatrix.fromArray(trainingFeatures)
    val b = RowPartitionedMatrix.fromArray(trainingLabels)
    val models = new NormalEquations().solveLeastSquaresWithManyL2(A, b, lambdas)
    val linearMappers = models.map(x => LinearMapper(x))
    linearMappers
  }
}

