package nodes.stats

import breeze.linalg.{argmax, DenseVector}
import org.apache.spark.rdd.RDD
import pipelines._

/** Returns the index of the largest input. */
case object MaxClassifier extends Transformer[DenseVector[Double], Int] {
  def apply(in: DenseVector[Double]): Int = argmax(in)
}

/** Returns the indices of the largest k inputs, in order. */
case class TopKClassifier(k: Int) extends Transformer[DenseVector[Double], Array[Int]] {
  def apply(vec: DenseVector[Double]): Array[Int] = {
    //TODO: This is SUPER inefficient.
    vec.toArray.toSeq.zipWithIndex.sortBy(_._1).takeRight(k).map(_._2).toArray
  }
}