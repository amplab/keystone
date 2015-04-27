package nodes.misc

import breeze.linalg.SparseVector
import org.apache.spark.rdd.RDD
import pipelines.{Estimator, Transformer}

/** A transformer which given a feature space, maps features of the form (feature id, value) into a sparse vector */
class SparseFeatureVectorizer(featureSpace: Map[Any, Int]) extends Transformer[Seq[(Any, Double)], SparseVector[Double]] {
  private def transformVector(in: Seq[(Any, Double)], featureSpaceMap: Map[Any, Int]): SparseVector[Double] = {
    val features = in.map(f => (featureSpaceMap.get(f._1), f._2))
        .filter(_._1.isDefined)
        .map(f => (f._1.get, f._2.toDouble))
    SparseVector(featureSpaceMap.size)(features:_*)
  }

  override def apply(in: RDD[Seq[(Any, Double)]]): RDD[SparseVector[Double]] = {
    val featureSpaceBroadcast = in.sparkContext.broadcast(featureSpace)
    in.map(transformVector(_, featureSpaceBroadcast.value))
  }
}

/**
 * An Estimator that chooses all sparse features observed when training,
 * and produces a transformer which builds a sparse vector out of them
 */
object AllSparseFeatures extends Estimator[RDD[Seq[(Any, Double)]], RDD[SparseVector[Double]]] {
  override def fit(data: RDD[Seq[(Any, Double)]]): SparseFeatureVectorizer = {
    val featureSpace = data.flatMap(_.map(_._1)).distinct()
        .zipWithIndex().collect().map(x => (x._1, x._2.toInt)).toMap
    new SparseFeatureVectorizer(featureSpace)
  }
}

/**
 * An Estimator that chooses the most frequently observed sparse features when training,
 * and produces a transformer which builds a sparse vector out of them
 *
 * @param numFeatures The number of features to keep
 */
case class CommonSparseFeatures(numFeatures: Int) extends Estimator[RDD[Seq[(Any, Double)]], RDD[SparseVector[Double]]] {
  override def fit(data: RDD[Seq[(Any, Double)]]): SparseFeatureVectorizer = {
    val featureSpace = data.flatMap(_.map(_._1)).countByValue().toSeq.sortBy(-_._2).take(numFeatures).map(_._1).zipWithIndex.toMap
    new SparseFeatureVectorizer(featureSpace)
  }
}