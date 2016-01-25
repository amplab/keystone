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
  private[workflow] def transform(dataDependencies: Seq[_]): T
  private[workflow] def transformRDD(dataDependencies: Seq[RDD[_]]): RDD[T]
}

private[workflow] case class DataNode(rdd: RDD[_]) extends Node {
  override def label: String = "%s[%d]".format(
    Option(rdd.name).map(_ + " ").getOrElse(""), rdd.id)
}

/**
  * A transformer used internally to apply the fit output of an Estimator onto data dependencies.
  * Takes one fit dependency, and directly applies its transform onto the data dependencies.
  *
  * Only expects one fit dependency. This is because the DSL will place a new DelegatingTransformer
  * after each Estimator whenever chaining an Estimator, and connect it via a fit dependency.
  */
private[workflow] class DelegatingTransformerNode(override val label: String)
  extends Node with Serializable