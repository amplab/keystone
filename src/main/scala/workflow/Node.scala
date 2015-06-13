package workflow

import org.apache.spark.rdd.RDD

sealed trait Node extends Serializable {
  def label: String = getClass.getSimpleName
}

abstract class EstimatorNode[T <: TransformerNode[_]] extends Node {
  def fit(dependencies: Seq[RDD[_]]): T
}

abstract class TransformerNode[T] extends Node {
  def transform(dataDependencies: Seq[_], fitDependencies: Seq[TransformerNode[_]]): T
  def transformRDD(dataDependencies: Seq[RDD[_]], fitDependencies: Seq[TransformerNode[_]]): RDD[T]
  def partialApply(fitDependencies: Seq[TransformerNode[_]]): TransformerNode[T]
}

case class DataNode[T](rdd: RDD[T]) extends Node {
  override def label: String = "%s[%d]".format(
    Option(rdd.name).map(_ + " ").getOrElse(""), rdd.id)
}
