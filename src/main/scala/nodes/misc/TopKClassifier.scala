package nodes.misc

import breeze.linalg.{argtopk, DenseVector}
import pipelines.Transformer

/**
 * Transformer that returns the indices of the largest k values of the vector, in order
 */
class TopKClassifier(k: Int) extends Transformer[DenseVector[Double], Seq[Int]] {
  override def apply(in: DenseVector[Double]): Seq[Int] = argtopk(in, k)
}

/**
 * Object to allow creating top k classifier w/o new
 */
object TopKClassifier {
  def apply(k: Int) = new TopKClassifier(k)
}
