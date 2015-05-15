package nodes.util

import breeze.linalg.{DenseVector, argmax}
import pipelines.Transformer

/**
 * Transformer that returns the index of the largest value in the vector
 */
object MaxClassifier extends Transformer[DenseVector[Double], Int] {
  override def apply(in: DenseVector[Double]): Int = argmax(in)
}
