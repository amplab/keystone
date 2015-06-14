package workflow

import org.apache.spark.rdd.RDD

sealed trait Node extends Serializable {
  def label: String = {
    val className = getClass.getSimpleName
    if (className endsWith "$") className.dropRight(1) else className
  }
}

abstract class EstimatorNode extends Node {
  def fit(dependencies: Seq[RDD[_]]): TransformerNode[_]
}

private[workflow] abstract class TransformerNode[T] extends Node {
  def transform(dataDependencies: Seq[_], fitDependencies: Seq[TransformerNode[_]]): T
  def transformRDD(dataDependencies: Seq[RDD[_]], fitDependencies: Seq[TransformerNode[_]]): RDD[T]
}

case class DataNode(rdd: RDD[_]) extends Node {
  override def label: String = "%s[%d]".format(
    Option(rdd.name).map(_ + " ").getOrElse(""), rdd.id)
}
