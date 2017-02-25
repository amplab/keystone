package keystoneml.nodes.learning

import breeze.linalg._
import breeze.stats._
import keystoneml.nodes.stats.StandardScalerModel
import org.apache.spark.rdd.RDD
import keystoneml.utils.MatrixUtils
import keystoneml.workflow.LabelEstimator

/**
 * Learns a linear model (OLS) based on training features and training labels.
 * Works well when the number of features >> number of examples, and the data fits locally.
 *
 * @param lambda regularization parameter
 */
class LocalLeastSquaresEstimator(lambda: Double)
    extends LabelEstimator[DenseVector[Double], DenseVector[Double], DenseVector[Double]] {

  override def fit(
    trainingFeatures: RDD[DenseVector[Double]],
    trainingLabels: RDD[DenseVector[Double]]): LinearMapper[DenseVector[Double]] = {
    LocalLeastSquaresEstimator.trainWithL2(trainingFeatures, trainingLabels, lambda)
  }
}

object LocalLeastSquaresEstimator {
  /**
   * Learns a linear model (OLS) based on training features and training labels.
   * Works well when the number of features >> number of examples.
   *
   * @param trainingFeatures Training features.
   * @param trainingLabels Training labels.
   * @return
   */
  def trainWithL2(
   trainingFeatures: RDD[DenseVector[Double]],
   trainingLabels: RDD[DenseVector[Double]],
   lambda: Double): LinearMapper[DenseVector[Double]] = {

    val A_parts = trainingFeatures.mapPartitions { x =>
      MatrixUtils.rowsToMatrixIter(x)
    }.collect()
    val b_parts = trainingLabels.mapPartitions { x =>
      MatrixUtils.rowsToMatrixIter(x)
    }.collect()

    val A_local = DenseMatrix.vertcat(A_parts:_*)
    val b_local = DenseMatrix.vertcat(b_parts:_*)

    val featuresMean = mean(A_local(::, *)).t
    val labelsMean = mean(b_local(::, *)).t

    val A_zm = A_local(*, ::) - featuresMean
    val b_zm = b_local(*, ::) - labelsMean

    val AAt = A_zm * A_zm.t
    val model = A_zm.t * ( (AAt + (DenseMatrix.eye[Double](AAt.rows) :* lambda)) \ b_zm )
    LinearMapper(model, Some(labelsMean), Some(new StandardScalerModel(featuresMean, None)))
  }

}
