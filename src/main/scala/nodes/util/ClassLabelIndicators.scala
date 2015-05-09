package nodes.util

import breeze.linalg.DenseVector
import org.apache.spark.rdd.RDD
import pipelines._

/**
 * Given a class label, returns a binary vector that indicates when that class is present.
 * @param numClasses
 */
case class ClassLabelIndicatorsFromIntLabels(numClasses: Int)
    extends Transformer[Int, DenseVector[Double]] {

  def apply(in: Int): DenseVector[Double] = {
    val indicatorVector = DenseVector.fill(numClasses, -1.0)
    indicatorVector(in) = 1.0
    indicatorVector
  }
}

/**
 * Given a set of class labels, returns a binary vector that indicates when each class is present.
 * @param numClasses
 */
case class ClassLabelIndicatorsFromIntArrayLabels(numClasses: Int)
    extends Transformer[Array[Int], DenseVector[Double]] {

  def apply(in: Array[Int]): DenseVector[Double] = {
    val indicatorVector = DenseVector.fill(numClasses, -1.0)
    var i = 0
    while (i < in.length) {
      indicatorVector(in(i)) = 1.0
      i += 1
    }
    indicatorVector
  }
}