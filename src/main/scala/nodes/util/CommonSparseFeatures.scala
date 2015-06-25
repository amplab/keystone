package nodes.util

import breeze.linalg.SparseVector
import org.apache.spark.rdd.RDD
import workflow.Estimator

import scala.reflect.ClassTag

/**
 * An Estimator that chooses the most frequently observed sparse features when training,
 * and produces a transformer which builds a sparse vector out of them
 *
 * Deterministically orders the feature mappings first by decreasing number of appearances,
 * then by earliest appearance in the RDD
 *
 * @param numFeatures The number of features to keep
 */
case class CommonSparseFeatures[T : ClassTag](numFeatures: Int) extends Estimator[Seq[(T, Double)], SparseVector[Double]] {
  // Ordering that compares (feature, frequency) pairs according to their frequencies
  val ordering = new Ordering[(T, (Int, Long))] {
    def compare(x: (T, (Int, Long)), y: (T, (Int, Long))): Int = {
      if (x._2._1 == y._2._1) {
        x._2._2.compare(y._2._2)
      } else {
        x._2._1.compare(y._2._1)
      }
    }
  }

  override def fit(data: RDD[Seq[(T, Double)]]): SparseFeatureVectorizer[T] = {
    val featureOccurrences = data.flatMap(identity).zipWithUniqueId().map(x => (x._1._1, (1, x._2)))
    // zip with unique ids and take the smallest unique id for a given feature to get
    // a deterministic ordering
    val featureFrequenciesWithUniqueId = featureOccurrences.reduceByKey {
      (x, y) => (x._1 + y._1, Math.min(x._2, y._2))
    }
    val mostCommonFeatures = featureFrequenciesWithUniqueId.top(numFeatures)(ordering).map(_._1)
    val featureSpace = mostCommonFeatures.zipWithIndex.toMap
    new SparseFeatureVectorizer(featureSpace)
  }
}
