package workflow.graph

import org.apache.spark.rdd.RDD

/**
 * An operator takes a sequence of [[Expression]]s as input and returns an [[Expression]].
 * One is stored internally inside each node of a [[Graph]], which represents how data
 * is intended to flow through the operators.
 */
private[graph] sealed trait Operator {
  def label: String = {
    val className = getClass.getSimpleName
    if (className endsWith "$") className.dropRight(1) else className
  }

  def execute(deps: Seq[Expression]): Expression
}

/**
 * An operator initialized with an [[RDD]], which takes no inputs and always outputs a
 * [[DatasetExpression]] containing that RDD, regardless of the inputs.
 *
 * @param rdd The RDD to always output
 */
private[graph] case class DatasetOperator(rdd: RDD[_]) extends Operator {
  override def label: String = "%s[%d]".format(
    Option(rdd.name).map(_ + " ").getOrElse(""), rdd.id)

  override def execute(deps: Seq[Expression]): DatasetExpression = {
    require(deps.isEmpty, "DatasetOperator does not take any inputs")
    new DatasetExpression(rdd)
  }
}

/**
 * An operator initialized with a single datum, which takes no inputs and always outputs a
 * [[DatumExpression]] containing that datum.
 *
 * @param datum The datum to always output
 */
private[graph] case class DatumOperator(datum: Any) extends Operator {
  override def label: String = {
    val className = datum.getClass.getSimpleName
    val datumName = if (className endsWith "$") className.dropRight(1) else className
    s"Datum [$datumName]"
  }

  override def execute(deps: Seq[Expression]): DatumExpression = {
    require(deps.isEmpty, "DatumOperator does not take any inputs")
    new DatumExpression(datum)
  }
}

/**
 * An operator that outputs a [[DatumExpression]] when fed a sequence of [[DatumExpression]]s as input,
 * and outputs a [[DatasetExpression]] when fed a sequence of [[DatasetExpression]]s as input.
 *
 * It has two methods that need to be implemented. singleTransform for going from a sequence
 * of single datums to a new datum, and batchTransform for going from a sequence of datasets
 * to a new dataset. When execute is called, it will check the types of the input and delegate
 * to the appropriate method.
 *
 * The operator is lazy, meaning that nothing will be executed until the
 * value of the [[Expression]] is accessed using `Output.get`.
 */
private[graph] abstract class TransformerOperator extends Operator with Serializable {
  /**
   * The single datum transformation, to go from a sequence of datums to a new single item.
   */
  private[graph] def singleTransform(inputs: Seq[DatumExpression]): Any

  /**
   * The batch dataset transformation, to go from a sequence of datasets to a new dataset.
   */
  private[graph] def batchTransform(inputs: Seq[DatasetExpression]): RDD[_]

  override def execute(deps: Seq[Expression]): Expression = {
    require(deps.nonEmpty, "Transformer dependencies may not be empty")
    require(deps.forall(_.isInstanceOf[DatasetExpression]) || deps.forall(_.isInstanceOf[DatumExpression]),
      "Transformer dependencies must be either all RDDs or all single data items")

    val depsAsRDD = deps.collect {
      case data: DatasetExpression => data
    }

    val depsAsDatum = deps.collect {
      case datum: DatumExpression => datum
    }

    if (depsAsRDD.nonEmpty) {
      // DatasetOutput is constructed as call-by-name,
      // meaning bulktransform will only be called when the output is used
      new DatasetExpression(batchTransform(depsAsRDD))
    } else {
      // DatumOutput is constructed as call-by-name,
      // meaning singleTransform will only be called when the output is used
      new DatumExpression(singleTransform(depsAsDatum))
    }
  }
}

/**
 * An operator that produces a [[TransformerExpression]] when fed a sequence of
 * [[DatasetExpression]]s.
 *
 * Implementations of this operator must implement `fitRDDs`, a method that takes a
 * [[Seq]] of [[DatasetExpression]]s and returns a [[TransformerOperator]].
 *
 * The operator is lazy, meaning that nothing will be executed until the
 * value of the [[Expression]] is accessed using `Output.get`.
 */
private[graph] abstract class EstimatorOperator extends Operator with Serializable {
  private[graph] def fitRDDs(inputs: Seq[DatasetExpression]): TransformerOperator

  override def execute(deps: Seq[Expression]): TransformerExpression = {
    val rdds = deps.collect {
      case data: DatasetExpression => data
    }
    require(rdds.size == deps.size)

    // TransformerOutput is constructed as call-by-name,
    // meaning fitRDDs will only be called when the transformer is used
    new TransformerExpression(fitRDDs(rdds))
  }
}

/**
 * This Operator is used to apply the output of an [[EstimatorOperator]] to new data.
 * It takes a [[TransformerExpression]] as its first dependency, accesses the contained
 * [[TransformerOperator]], and delegates the rest of its dependencies to that Transformer.
 *
 * The operator is lazy, meaning that nothing will be executed until the
 * value of the [[Expression]] is accessed using `Output.get`.
 */
private[graph] class DelegatingOperator extends Operator with Serializable {
  override def execute(deps: Seq[Expression]): Expression = {
    require(deps.nonEmpty, "DelegatingOperator dependencies may not be empty")
    require(deps.tail.nonEmpty, "Transformer dependencies may not be empty")
    require(deps.tail.forall(_.isInstanceOf[DatasetExpression]) || deps.tail.forall(_.isInstanceOf[DatumExpression]),
      "Transformer dependencies must be either all RDDs or all single data items")

    val transformer = deps.collectFirst {
      case t: TransformerExpression => t
    }.get

    val depsAsRDD = deps.tail.collect {
      case data: DatasetExpression => data
    }

    val depsAsDatum = deps.tail.collect {
      case datum: DatumExpression => datum
    }

    if (depsAsRDD.nonEmpty) {
      // DatasetOutput is constructed as call-by-name,
      // meaning bulktransform will only be called when the output is used
      new DatasetExpression(transformer.get.batchTransform(depsAsRDD))
    } else {
      // DatumOutput is constructed as call-by-name,
      // meaning singleTransform will only be called when the output is used
      new DatumExpression(transformer.get.singleTransform(depsAsDatum))
    }
  }
}
