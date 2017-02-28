package keystoneml.nodes.stats

import breeze.linalg.DenseVector
import keystoneml.pipelines._
import keystoneml.workflow.Transformer

/**
 * This transformer applies a Linear Rectifier,
 * an activation function defined as:
 * f(x) = max({@param maxVal}, x - {@param alpha})
 */
case class LinearRectifier(maxVal: Double = 0.0, alpha: Double = 0.0)
  extends Transformer[DenseVector[Double], DenseVector[Double]] {
  def apply(in: DenseVector[Double]): DenseVector[Double] = {
    in.map(e => math.max(maxVal, e - alpha))
  }
}
