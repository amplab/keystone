package workflow.old

import org.apache.spark.rdd.RDD

sealed trait Instruction {
  def getDependencies: Seq[Int]
  def mapDependencies(func: Int => Int): Instruction
  def execute(deps: Seq[InstructionOutput]): InstructionOutput
}

sealed trait InstructionOutput
abstract class DataOutput extends InstructionOutput
case class RDDOutput(rdd: RDD[_]) extends DataOutput
case class SingleDataOutput(data: Any) extends DataOutput
case class EstimatorOutput(estimatorNode: EstimatorNode) extends InstructionOutput
case class TransformerOutput(transformerNode: TransformerNode) extends InstructionOutput

/**
 * TransformerApplyNode is an Instruction that represents the
 * application of a transformer to data inputs.
 *
 * @param transformer The index of the [[TransformerNode]] in the instructions
 * @param inputs The indices of the data inputs in the instructions
 */
private[workflow] case class TransformerApplyNode(transformer: Int, inputs: Seq[Int]) extends Instruction {
  override def getDependencies: Seq[Int] = Seq(transformer) ++ inputs
  override def mapDependencies(func: (Int) => Int): TransformerApplyNode = {
    TransformerApplyNode(func(transformer), inputs.map(func))
  }

  override def execute(deps: Seq[InstructionOutput]): DataOutput = {
    val transformer = deps.collectFirst {
      case TransformerOutput(t) => t
    }.get

    val dataInputs = deps.tail.collect {
      case RDDOutput(rdd) => rdd
    }

    RDDOutput(transformer.transformRDD(dataInputs.toIterator))
  }
}

/**
 * An Instruction that represents the fitting of an estimator with data inputs.
 *
 * @param est The index of the [[EstimatorNode]] in the instructions
 * @param inputs The indices of the data inputs in the instructions
 */
private[workflow] case class EstimatorFitNode(est: Int, inputs: Seq[Int]) extends Instruction {
  override def getDependencies: Seq[Int] = Seq(est) ++ inputs
  override def mapDependencies(func: (Int) => Int): EstimatorFitNode = {
    EstimatorFitNode(func(est), inputs.map(func))
  }

  override def execute(deps: Seq[InstructionOutput]): TransformerOutput = {
    val estimator = deps.collectFirst {
      case EstimatorOutput(e) => e
    }.get

    val dataInputs = deps.tail.collect {
      case RDDOutput(rdd) => rdd
    }

    TransformerOutput(estimator.fitRDDs(dataInputs.toIterator))
  }
}

sealed trait Node  {
  def label: String = {
    val className = getClass.getSimpleName
    if (className endsWith "$") className.dropRight(1) else className
  }
}

private[workflow] abstract class EstimatorNode extends Node with Serializable with Instruction {
  private[workflow] def fitRDDs(dependencies: Iterator[RDD[_]]): TransformerNode
  override def getDependencies: Seq[Int] = Seq()
  override def mapDependencies(func: (Int) => Int): EstimatorNode = this
  override def execute(deps: Seq[InstructionOutput]): EstimatorOutput = EstimatorOutput(this)
}

private[workflow] abstract class TransformerNode extends Node with Serializable with Instruction {
  private[workflow] def transform(dataDependencies: Iterator[_]): Any
  private[workflow] def transformRDD(dataDependencies: Iterator[RDD[_]]): RDD[_]

  override def getDependencies: Seq[Int] = Seq()
  override def mapDependencies(func: (Int) => Int): TransformerNode = this
  override def execute(deps: Seq[InstructionOutput]): TransformerOutput = TransformerOutput(this)
}

private[workflow] case class SourceNode(rdd: RDD[_]) extends Node with Instruction {
  override def label: String = "%s[%d]".format(
    Option(rdd.name).map(_ + " ").getOrElse(""), rdd.id)
  override def getDependencies: Seq[Int] = Seq()
  override def mapDependencies(func: (Int) => Int): SourceNode = this
  override def execute(deps: Seq[InstructionOutput]): RDDOutput = {
    RDDOutput(rdd)
  }
}

/**
 * A node used internally to apply the fit output of an Estimator onto data dependencies.
 * Takes one fit dependency, and directly applies its transform onto the data dependencies.
 *
 * Only expects one fit dependency. This is because the DSL will place a new DelegatingTransformer
 * after each Estimator whenever chaining an Estimator, and connect it via a fit dependency.
 */
private[workflow] class DelegatingTransformerNode(override val label: String)
  extends Node with Serializable