package nodes.learning

import breeze.linalg._
import edu.berkeley.cs.amplab.mlmatrix.{NormalEquations, RowPartitionedMatrix}
import org.apache.spark.rdd.RDD
import pipelines.Pipelines._
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
   * @param in Collection of A's.
   * @return Collection of B's.
   */
  def apply(in: RDD[Array[Double]]): RDD[Array[Double]] = {
    val modelBroadcast = in.context.broadcast(x)
    in.mapPartitions(rows => {
      val mat = DenseMatrix(rows.toSeq:_*) * modelBroadcast.value
      MatrixUtils.matrixToRowArray(mat).iterator
    })
  }
}
/**
 * Linear Map Estimator. Solves an OLS problem on data given labels and emits a LinearMapper transformer.
 *
 * @param lambda L2 Regularization parameter
 */
class LinearMapEstimator(lambda: Option[Double] = None)
    extends LabelEstimator[RDD[Array[Double]], RDD[Array[Double]], RDD[Array[Double]]]
    with Serializable {

  /**
   * Learns a linear model (OLS) based on training features and training labels.
   * If the regularization parameter is set
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

    val x = lambda match {
      case Some(l) => new NormalEquations().solveLeastSquaresWithL2(A, b, l)
      case None => new NormalEquations().solveLeastSquares(A, b)
    }

    LinearMapper(x)
  }
}

/**
 * Companion object to LinearMapEstimator that allows for construction without new.
 */
object LinearMapEstimator extends Serializable {
  def apply(lambda: Option[Double] = None) = new LinearMapEstimator(lambda)
}
