package nodes.learning

import breeze.linalg._
import edu.berkeley.cs.amplab.mlmatrix.{NormalEquations, RowPartitionedMatrix}
import nodes.stats.{StandardScaler, StandardScalerModel}
import org.apache.spark.rdd.RDD
import utils.MatrixUtils
import workflow.{LabelEstimator, Transformer}

/**
 * Computes A * x + b i.e. a linear map of data using a trained model.
 *
 * @param x trained model
 * @param bOpt optional intercept to add
 * @param featureScaler optional scaler to apply to data before applying the model
 */
case class LinearMapper(
    x: DenseMatrix[Double],
    bOpt: Option[DenseVector[Double]] = None,
    featureScaler: Option[StandardScalerModel] = None)
  extends Transformer[DenseVector[Double], DenseVector[Double]] {

  /**
   * Apply a linear model to an input.
   * @param in Input.
   * @return Output.
   */
  def apply(in: DenseVector[Double]): DenseVector[Double] = {
    val scaled = featureScaler.map(_.apply(in)).getOrElse(in)
    val out = x.t * scaled
    bOpt.map { b =>
      out :+= b
    }.getOrElse(out)
  }

  /**
   * Apply a linear model to a collection of inputs.
   *
   * @param in Collection of A's.
   * @return Collection of B's.
   */
  override def apply(in: RDD[DenseVector[Double]]): RDD[DenseVector[Double]] = {
    val modelBroadcast = in.context.broadcast(x)
    val bBroadcast = in.context.broadcast(bOpt)
    val inScaled = featureScaler.map(_.apply(in)).getOrElse(in)
    inScaled.mapPartitions(rows => {
      val mat = MatrixUtils.rowsToMatrix(rows) * modelBroadcast.value
      val out = bBroadcast.value.map { b =>
        mat(*, ::) :+= b
        mat
      }.getOrElse(mat)

      MatrixUtils.matrixToRowArray(out).iterator
    })
  }
}
/**
 * Linear Map Estimator. Solves an OLS problem on data given labels and emits a LinearMapper transformer.
 *
 * @param lambda L2 Regularization parameter
 */
class LinearMapEstimator(lambda: Option[Double] = None)
    extends LabelEstimator[DenseVector[Double], DenseVector[Double], DenseVector[Double]] {

  /**
   * Learns a linear model (OLS) based on training features and training labels.
   * If the regularization parameter is set
   *
   * @param trainingFeatures Training features.
   * @param trainingLabels Training labels.
   * @return
   */
  def fit(
      trainingFeatures: RDD[DenseVector[Double]],
      trainingLabels: RDD[DenseVector[Double]]): LinearMapper = {

    val featureScaler = new StandardScaler(normalizeStdDev = false).fit(trainingFeatures)
    val labelScaler = new StandardScaler(normalizeStdDev = false).fit(trainingLabels)

    val A = RowPartitionedMatrix.fromArray(
      featureScaler.apply(trainingFeatures).map(x => x.toArray))
    val b = RowPartitionedMatrix.fromArray(
      labelScaler.apply(trainingLabels).map(x => x.toArray))

    val x = lambda match {
      case Some(l) => new NormalEquations().solveLeastSquaresWithL2(A, b, l)
      case None => new NormalEquations().solveLeastSquares(A, b)
    }

    LinearMapper(x, Some(labelScaler.mean), Some(featureScaler))
  }
}

/**
 * Companion object to LinearMapEstimator that allows for construction without new.
 */
object LinearMapEstimator extends Serializable {
  def apply(lambda: Option[Double] = None) = new LinearMapEstimator(lambda)
}
