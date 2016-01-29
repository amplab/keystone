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
case class LinearMapper[T <: Vector[Double]](
    x: DenseMatrix[Double],
    bOpt: Option[DenseVector[Double]] = None,
    featureScaler: Option[StandardScalerModel] = None)
  extends Transformer[T, DenseVector[Double]] {

  /**
   * Apply a linear model to an input.
   * @param in Input.
   * @return Output.
   */
  def apply(in: T): DenseVector[Double] = {
    val scaled = featureScaler.map(_.apply(in match {
      case dense: DenseVector[Double] => dense
      case _ => in.toDenseVector
    })).getOrElse(in)

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
  override def apply(in: RDD[T]): RDD[DenseVector[Double]] = {
    val modelBroadcast = in.context.broadcast(x)
    val bBroadcast = in.context.broadcast(bOpt)

    val inScaled = featureScaler.map(_.apply(in.map {
      case dense: DenseVector[Double] => dense
      case notDense => notDense.toDenseVector
    })).getOrElse(in)

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
case class LinearMapEstimator[T <: Vector[Double]](lambda: Option[Double] = None)
    extends SolverWithCostModel[T] {

  /**
   * Learns a linear model (OLS) based on training features and training labels.
   * If the regularization parameter is set
   *
   * @param trainingFeatures Training features.
   * @param trainingLabels Training labels.
   * @return
   */
  def fit(
      trainingFeatures: RDD[T],
      trainingLabels: RDD[DenseVector[Double]]): LinearMapper[T] = {

    val denseTrainingFeatures = trainingFeatures.map {
      case dense: DenseVector[Double] => dense
      case notDense => notDense.toDenseVector
    }

    val featureScaler = new StandardScaler(normalizeStdDev = false).fit(denseTrainingFeatures)
    val labelScaler = new StandardScaler(normalizeStdDev = false).fit(trainingLabels)

    val A = RowPartitionedMatrix.fromArray(
      featureScaler.apply(denseTrainingFeatures).map(x => x.toArray))
    val b = RowPartitionedMatrix.fromArray(
      labelScaler.apply(trainingLabels).map(x => x.toArray))

    val x = lambda match {
      case Some(l) => new NormalEquations().solveLeastSquaresWithL2(A, b, l)
      case None => new NormalEquations().solveLeastSquares(A, b)
    }

    LinearMapper(x, Some(labelScaler.mean), Some(featureScaler))
  }

  override def cost(
    n: Long,
    d: Int,
    k: Int,
    sparsity: Double,
    numMachines: Int,
    cpuWeight: Double,
    memWeight: Double,
    networkWeight: Double)
  : Double = {
    val flops = n.toDouble * d * (d + k) / numMachines.toDouble
    val bytesScanned = n.toDouble * d / numMachines.toDouble + (d.toDouble * d)
    val network = d.toDouble * (d + k)

    math.max(cpuWeight * flops, memWeight * bytesScanned) + networkWeight * network
  }
}

/**
 * Companion object to LinearMapEstimator.
 */
object LinearMapEstimator extends Serializable {
  def computeCost(
      trainingFeatures: RDD[DenseVector[Double]],
      trainingLabels: RDD[DenseVector[Double]],
      lambda: Double,
      x: DenseMatrix[Double],
      bOpt: Option[DenseVector[Double]]): Double = {

    val nTrain = trainingLabels.count
    val modelBroadcast = trainingLabels.context.broadcast(x)
    val bBroadcast = trainingLabels.context.broadcast(bOpt)

    val axb = trainingFeatures.mapPartitions(rows => {
      val mat = MatrixUtils.rowsToMatrix(rows) * modelBroadcast.value
      val out = bBroadcast.value.map { b =>
        mat(*, ::) :+= b
        mat
      }.getOrElse(mat)

      MatrixUtils.matrixToRowArray(out).iterator
    })

    val cost = axb.zip(trainingLabels).map { part =>
      val axb = part._1
      val labels = part._2
      val out = axb - labels
      math.pow(norm(out), 2)
    }.reduce(_ + _)

    if (lambda == 0) {
      cost/(2.0*nTrain.toDouble)
    } else {
      val wNorm = math.pow(norm(x.toDenseVector), 2)
      cost/(2.0*nTrain.toDouble) + lambda/2.0 * wNorm
    }
  }
}
