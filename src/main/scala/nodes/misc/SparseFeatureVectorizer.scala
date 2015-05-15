package nodes.misc

import breeze.linalg.SparseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD._
import pipelines.{Estimator, Transformer}

/** A transformer which given a feature space, maps features of the form (feature id, value) into a sparse vector */
class SparseFeatureVectorizer(featureSpace: Map[Any, Int]) extends Transformer[Seq[(Any, Double)], SparseVector[Double]] {
  private def transformVector(in: Seq[(Any, Double)], featureSpaceMap: Map[Any, Int]): SparseVector[Double] = {
    val features = in.map(f => (featureSpaceMap.get(f._1), f._2))
        .filter(_._1.isDefined)
        .map(f => (f._1.get, f._2.toDouble))
    SparseVector(featureSpaceMap.size)(features:_*)
  }

  override def apply(in: Seq[(Any, Double)]): SparseVector[Double] = {
    transformVector(in, featureSpace)
  }
}

/**
 * An Estimator that chooses all sparse features observed when training,
 * and produces a transformer which builds a sparse vector out of them
 */
object AllSparseFeatures extends Estimator[Seq[(Any, Double)], SparseVector[Double]] {
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
case class CommonSparseFeatures(numFeatures: Int) extends Estimator[Seq[(Any, Double)], SparseVector[Double]] {
  // Ordering that compares (feature, frequency) pairs according to their frequencies
  val ordering = new Ordering[(Any, Int)] {
    override def compare(x: (Any, Int), y: (Any, Int)): Int = x._2.compare(y._2)
  }

  override def fit(data: RDD[Seq[(Any, Double)]]): SparseFeatureVectorizer = {
    val featureFrequencies = data.flatMap(identity).mapValues(_ => 1).reduceByKey(_+_)
    val mostCommonFeatures = featureFrequencies.top(numFeatures)(ordering).map(_._1)
    val featureSpace = mostCommonFeatures.zipWithIndex.toMap
    new SparseFeatureVectorizer(featureSpace)
  }
}