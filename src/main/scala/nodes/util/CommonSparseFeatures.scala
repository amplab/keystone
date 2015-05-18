package nodes.util

import breeze.linalg.SparseVector
import org.apache.spark.rdd.RDD
import pipelines.Estimator

import scala.reflect.ClassTag

/**
 * An Estimator that chooses the most frequently observed sparse features when training,
 * and produces a transformer which builds a sparse vector out of them
 *
 * @param numFeatures The number of features to keep
 */
case class CommonSparseFeatures[T : ClassTag](numFeatures: Int) extends Estimator[Seq[(T, Double)], SparseVector[Double]] {
  // Ordering that compares (feature, frequency) pairs according to their frequencies
  val ordering = new Ordering[(T, Int)] {
    override def compare(x: (T, Int), y: (T, Int)): Int = x._2.compare(y._2)
  }

  override def fit(data: RDD[Seq[(T, Double)]]): SparseFeatureVectorizer[T] = {
    val featureFrequencies = data.flatMap(identity).mapValues(_ => 1).reduceByKey(_+_)
    val mostCommonFeatures = featureFrequencies.top(numFeatures)(ordering).map(_._1)
    val featureSpace = mostCommonFeatures.zipWithIndex.toMap
    new SparseFeatureVectorizer(featureSpace)
  }
}
