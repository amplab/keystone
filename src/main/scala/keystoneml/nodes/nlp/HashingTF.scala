package keystoneml.nodes.nlp

import breeze.linalg.SparseVector
import keystoneml.workflow.Transformer

/**
 * Converts a sequence of terms to a sparse vector representing their frequencies,
 * using the hashing trick: https://en.wikipedia.org/wiki/Feature_hashing
 *
 * Terms are hashed using Scala's `.##` method. We may want to convert to MurmurHash3 for strings,
 * as discussed for Spark's ML Pipelines in https://issues.apache.org/jira/browse/SPARK-10574
 *
 * @param numFeatures The desired feature space to convert to using the hashing trick.
 */
case class HashingTF[T <: Seq[Any]](numFeatures: Int) extends Transformer[T, SparseVector[Double]] {
  def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }

  def apply(document: T): SparseVector[Double] = {
    val termFrequencies = scala.collection.mutable.HashMap.empty[Int, Double]
    document.foreach { term =>
      val i = nonNegativeMod(term.##, numFeatures)
      termFrequencies.put(i, termFrequencies.getOrElse(i, 0.0) + 1.0)
    }

    SparseVector(numFeatures)(termFrequencies.toSeq:_*)
  }
}

