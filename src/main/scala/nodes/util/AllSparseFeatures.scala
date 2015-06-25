package nodes.util

import breeze.linalg.SparseVector
import org.apache.spark.rdd.RDD
import workflow.Estimator

import scala.reflect.ClassTag

/**
 * An Estimator that chooses all sparse features observed when training,
 * and produces a transformer which builds a sparse vector out of them.
 *
 * Deterministically orders the feature mappings by earliest appearance in the RDD
 */
case class AllSparseFeatures[T: ClassTag]() extends Estimator[Seq[(T, Double)], SparseVector[Double]] {
  override def fit(data: RDD[Seq[(T, Double)]]): SparseFeatureVectorizer[T] = {
    val featureOccurrences = data.flatMap(_.map(_._1))
    // zip with unique ids and take the smallest unique id for a given feature to get
    // a deterministic ordering
    val featuresWithUniqueId = featureOccurrences.zipWithUniqueId().reduceByKey {
      (x, y) => Math.min(x, y)
    }
    val featureSpace = featuresWithUniqueId.sortBy(_._2).map(_._1)
        .collect().zipWithIndex.toMap
    new SparseFeatureVectorizer(featureSpace)
  }
}
