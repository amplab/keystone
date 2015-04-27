package nodes.misc

import breeze.linalg.SparseVector
import org.apache.spark.rdd.RDD
import pipelines.{Estimator, Transformer}

/**
 * A transformer which given a feature space, maps sparse features of the form (identifier, value) into a vector
 */
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
 * A simple feature selector that chooses all features produced by a sparse feature extractor,
 * and produces a transformer which builds a sparse vector out of all the features it sees in the corpus
 */
object AllSparseFeatures extends Estimator[RDD[Seq[(Any, Double)]], RDD[SparseVector[Double]]] {
  override def fit(data: RDD[Seq[(Any, Double)]]): SparseFeatureVectorizer = {
    val featureSpace = data.flatMap(_.map(_._1)).distinct()
        .zipWithIndex().collect().map(x => (x._1, x._2.toInt)).toMap
    new SparseFeatureVectorizer(featureSpace)
  }
}

/**
 * A feature selector that keeps the most frequently observed sparse features,
 * and produces a transformer which builds a sparse vector out of these
 *
 * @param numFeatures The number of features to keep
 */
case class CommonSparseFeatures(numFeatures: Int) extends Estimator[RDD[Seq[(Any, Double)]], RDD[SparseVector[Double]]] {
  override def fit(data: RDD[Seq[(Any, Double)]]): SparseFeatureVectorizer = {
    val featureSpace = data.flatMap(_.map(_._1)).countByValue().toSeq.sortBy(-_._2).take(numFeatures).map(_._1).zipWithIndex.toMap
    new SparseFeatureVectorizer(featureSpace)
  }
}