package nodes.util

import breeze.linalg.DenseVector
import org.apache.spark.rdd.RDD
import pipelines._
import workflow.Transformer

/**
 * Given a class label, returns a binary vector that indicates when that class is present.
 *
 * Expects labels in the range [1, numClasses).
 *
 * @param numClasses
 */
case class ClassLabelIndicatorsFromIntLabels(numClasses: Int)
    extends Transformer[Int, DenseVector[Double]] {

  assert(numClasses > 1, "numClasses must be > 1.")

  def apply(in: Int): DenseVector[Double] = {
    if(in < 0 || in >= numClasses) {
      throw new RuntimeException("Class labels are expected to be in the range [0, numClasses)")
    }

    val indicatorVector = DenseVector.fill(numClasses, -1.0)
    indicatorVector(in) = 1.0
    indicatorVector
  }
}

/**
 * Given a set of class labels, returns a binary vector that indicates when each class is present.
 *
 * Expects labels in the range [1, numClasses).
 *
 * @param numClasses
 */
case class ClassLabelIndicatorsFromIntArrayLabels(numClasses: Int, validate: Boolean = false)
    extends Transformer[Array[Int], DenseVector[Double]] {

  assert(numClasses > 1, "numClasses must be > 1.")

  def apply(in: Array[Int]): DenseVector[Double] = {
    if(validate && (in.max > numClasses || in.min < 0)) {
      throw new RuntimeException("Class labels are expected to be in the range [0, numClasses)")
    }

    val indicatorVector = DenseVector.fill(numClasses, -1.0)
    var i = 0
    while (i < in.length) {
      indicatorVector(in(i)) = 1.0
      i += 1
    }
    indicatorVector
  }
}