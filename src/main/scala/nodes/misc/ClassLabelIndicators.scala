package nodes.misc

import breeze.linalg.DenseVector
import pipelines.Transformer

/**
 * Transformer that converts an Int class label into a vector
 * where the value is -1.0 for all indices except that label,
 * which has value 1.0
 */
case class ClassLabelIndicators(numClasses: Int)
    extends Transformer[Int, DenseVector[Double]]
    with Serializable {
  override def apply(in: Int): DenseVector[Double] = {
    val indicatorVector = DenseVector.fill(numClasses, -1.0)
    indicatorVector(in) = 1.0
    indicatorVector
  }
}
