package nodes.learning

import breeze.linalg._
import nodes.learning.Gradient.{SparseGradient, DenseGradient}
import utils.MatrixUtils

/**
  * Created by tomerk11 on 1/19/16.
  */
trait Gradient[T <: Vector[Double]] extends Serializable {

  def compute(
    numFeatures: Int,
    numClasses: Int,
    data: Iterator[T],
    labels: Iterator[DenseVector[Double]],
    weights: DenseMatrix[Double])
  : (DenseMatrix[Double], Double)
}

object Gradient {
  type DenseGradient = Gradient[DenseVector[Double]]
  type SparseGradient = Gradient[SparseVector[Double]]
}

class LeastSquaresDenseGradient extends DenseGradient {

  def compute(
    numFeatures: Int,
    numClasses: Int,
    data: Iterator[DenseVector[Double]],
    labels: Iterator[DenseVector[Double]],
    weights: DenseMatrix[Double])
  : (DenseMatrix[Double], Double) = {
    if (data.hasNext) {
      val dataMatrix = MatrixUtils.rowsToMatrix(data)
      val labelsMatrix = MatrixUtils.rowsToMatrix(labels)

      // Least Squares Gradient is At.(Ax - b)
      val axb = dataMatrix * weights - labelsMatrix
      val grad = dataMatrix.t * (axb)
      // Loss is 0.5 * norm(Ax - b)
      val loss = 0.5 * math.pow(norm(axb.toDenseVector), 2)

      (grad, loss)
    } else {
      (DenseMatrix.zeros[Double](numFeatures, numClasses), 0)
    }
  }
}

class LeastSquaresSparseGradient extends SparseGradient {
  override def compute(
    numFeatures: Int,
    numClasses: Int,
    data: Iterator[SparseVector[Double]],
    labels: Iterator[DenseVector[Double]],
    weights: DenseMatrix[Double])
  : (DenseMatrix[Double], Double) = {
    val gradient = DenseMatrix.zeros[Double](numFeatures, numClasses)
    var loss = 0.0

    while (data.hasNext) {
      val feature = data.next()
      val label = labels.next()

      // Least Squares Gradient is At.(Ax - b)
      val axb = weights.t * feature
      axb -= label

      if (label.length == 1) {
        // Performance optimization for the binary case
        // Data is  dx1

        // axb is 0
        var axb = 0.0

        var offset = 0
        while(offset < feature.activeSize) {
          val index = feature.indexAt(offset)
          val value = feature.valueAt(offset)
          axb += weights.data(index) * value
          offset += 1
        }

        axb -= label(0)

        offset = 0
        while(offset < feature.activeSize) {
          val index = feature.indexAt(offset)
          val value = feature.valueAt(offset)
          val gradUpdate = (axb * value)
          gradient(index, 0) += gradUpdate
          offset += 1
        }

        loss = loss + 0.5 * axb * axb

      } else {
        var offset = 0
        while (offset < feature.activeSize) {
          val index = feature.indexAt(offset)
          val value = feature.valueAt(offset)
          gradient(index, ::) += (axb.t * value)
          offset += 1
        }
        loss = loss + 0.5 * math.pow(norm(axb), 2)
      }
    }

    (gradient, loss)
  }
}
