package workflow

import org.apache.spark.rdd.RDD

sealed trait Instruction {
  def getDependencies: Seq[Int]
  def mapDependencies(func: Int => Int): Instruction
}

case class SourceNode(rdd: RDD[_]) extends Instruction {
  override def getDependencies: Seq[Int] = Seq()
  override def mapDependencies(func: (Int) => Int): SourceNode = this
}

case class TransformerApplyNode(transformer: Int, inputs: Seq[Int]) extends Instruction {
  override def getDependencies: Seq[Int] = Seq(transformer) ++ inputs
  override def mapDependencies(func: (Int) => Int): TransformerApplyNode = {
    TransformerApplyNode(func(transformer), inputs.map(func))
  }
}

case class EstimatorFitNode(est: Int, inputs: Seq[Int]) extends Instruction {
  override def getDependencies: Seq[Int] = Seq(est) ++ inputs
  override def mapDependencies(func: (Int) => Int): EstimatorFitNode = {
    EstimatorFitNode(func(est), inputs.map(func))
  }
}

sealed trait Node  {
  def label: String = {
    val className = getClass.getSimpleName
    if (className endsWith "$") className.dropRight(1) else className
  }
}

private[workflow] abstract class EstimatorNode extends Node with Serializable with Instruction {
  private[workflow] def fit(dependencies: Seq[RDD[_]]): TransformerNode
  override def getDependencies: Seq[Int] = Seq()
  override def mapDependencies(func: (Int) => Int): EstimatorNode = this
}

private[workflow] abstract class TransformerNode extends Node with Serializable with Instruction {
  private[workflow] def transform(dataDependencies: Seq[_]): Any
  private[workflow] def transformRDD(dataDependencies: Seq[RDD[_]]): RDD[_]

  override def getDependencies: Seq[Int] = Seq()
  override def mapDependencies(func: (Int) => Int): TransformerNode = this
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