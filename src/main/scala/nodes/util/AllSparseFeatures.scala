package nodes.util

import breeze.linalg.SparseVector
import org.apache.spark.rdd.RDD
import pipelines.Estimator

import scala.reflect.ClassTag

/**
 * An Estimator that chooses all sparse features observed when training,
 * and produces a transformer which builds a sparse vector out of them
 */
case class AllSparseFeatures[T: ClassTag]() extends Estimator[Seq[(T, Double)], SparseVector[Double]] {
  override def fit(data: RDD[Seq[(T, Double)]]): SparseFeatureVectorizer[T] = {
    val featureSpace = data.flatMap(_.map(_._1)).distinct()
        .zipWithIndex().collect().map(x => (x._1, x._2.toInt)).toMap
    new SparseFeatureVectorizer(featureSpace)
  }
}
