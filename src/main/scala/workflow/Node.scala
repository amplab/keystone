package workflow

import org.apache.spark.rdd.RDD

sealed trait Node  {
  def label: String = {
    val className = getClass.getSimpleName
    if (className endsWith "$") className.dropRight(1) else className
  }
}

private[workflow] abstract class EstimatorNode extends Node with Serializable {
  private[workflow] def fit(dependencies: Seq[RDD[_]]): TransformerNode[_]
}

private[workflow] abstract class TransformerNode[T] extends Node with Serializable {
  private[workflow] def transform(dataDependencies: Seq[_], fitDependencies: Seq[TransformerNode[_]]): T
  private[workflow] def transformRDD(dataDependencies: Seq[RDD[_]], fitDependencies: Seq[TransformerNode[_]]): RDD[T]
}

private[workflow] case class DataNode(rdd: RDD[_]) extends Node {
  override def label: String = "%s[%d]".format(
    Option(rdd.name).map(_ + " ").getOrElse(""), rdd.id)
}
