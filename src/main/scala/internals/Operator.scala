package internals

import org.apache.spark.rdd.RDD

/**
 * An operator takes a sequence of [[Output]]s as input and returns an [[Output]].
 * One is stored internally inside each node of a [[Graph]], which represents how data
 * is intended to flow through the operators.
 */
private[internals] sealed trait Operator {
  def label: String = {
    val className = getClass.getSimpleName
    if (className endsWith "$") className.dropRight(1) else className
  }

  def execute(deps: Seq[Output]): Output
}

/**
 * An operator initialized with an [[RDD]], which always outputs a [[DatasetOutput]] containing
 * that RDD, regardless of the inputs.
 *
 * @param rdd The RDD to always output
 */
private[internals] case class DatasetOperator(rdd: RDD[_]) extends Operator {
  override def label: String = "%s[%d]".format(
    Option(rdd.name).map(_ + " ").getOrElse(""), rdd.id)

  override def execute(deps: Seq[Output]): DatasetOutput = {
    new DatasetOutput(rdd)
  }
}

/**
 * An operator initialized with a single datum, which always outputs a [[DatumOutput]] containing
 * that datum, regardless of the inputs.
 *
 * @param datum The datum to always output
 */
private[internals] case class DatumOperator(datum: Any) extends Operator {
  override def label: String = {
    val className = datum.getClass.getSimpleName
    val datumName = if (className endsWith "$") className.dropRight(1) else className
    s"Datum [$datumName]"
  }

  override def execute(deps: Seq[Output]): DatumOutput = {
    new DatumOutput(datum)
  }
}

/**
 * An operator that outputs a [[DatumOutput]] when fed a sequence of [[DatumOutput]]s as input,
 * and outputs a [[DatasetOutput]] when fed a sequence of [[DatasetOutput]]s as input.
 *
 * It has two methods that need to be implemented. singleTransform for going from a sequence
 * of single datums to a new datum, and batchTransform for going from a sequence of datasets
 * to a new dataset. When execute is called, it will check the types of the input and delegate
 * to the appropriate method.
 *
 * The operator is lazy, meaning that nothing will be executed until the
 * value of the [[Output]] is accessed using `Output.get`.
 */
private[internals] abstract class TransformerOperator extends Operator with Serializable {
  /**
   * The single datum transformation, to go from a sequence of datums to a new single item.
   */
  private[internals] def singleTransform(dataDependencies: Seq[DatumOutput]): Any

  /**
   * The batch dataset transformation, to go from a sequence of datasets to a new dataset.
   */
  private[internals] def batchTransform(dataDependencies: Seq[DatasetOutput]): RDD[_]

  override def execute(deps: Seq[Output]): Output = {
    require(deps.forall(_.isInstanceOf[DatasetOutput]) || deps.forall(_.isInstanceOf[DatumOutput]),
      "Transformer dependencies must be either all RDDs or all single data items")

    val depsAsRDD = deps.collect {
      case data: DatasetOutput => data
    }

    val depsAsDatum = deps.collect {
      case datum: DatumOutput => datum
    }

    if (depsAsRDD.nonEmpty) {
      // DatasetOutput is constructed as call-by-name,
      // meaning bulktransform will only be called when the output is used
      new DatasetOutput(batchTransform(depsAsRDD))
    } else {
      // DatumOutput is constructed as call-by-name,
      // meaning singleTransform will only be called when the output is used
      new DatumOutput(singleTransform(depsAsDatum))
    }
  }
}

/**
 * An operator that produces a [[TransformerOutput]] when fed a sequence of
 * [[DatasetOutput]]s.
 *
 * Implementations of this operator must implement `fitRDDs`, a method that takes a
 * [[Seq]] of [[DatasetOutput]]s and returns a [[TransformerOperator]].
 *
 * The operator is lazy, meaning that nothing will be executed until the
 * value of the [[Output]] is accessed using `Output.get`.
 */
private[internals] abstract class EstimatorOperator extends Operator with Serializable {
  private[internals] def fitRDDs(in: Seq[DatasetOutput]): TransformerOperator

  override def execute(deps: Seq[Output]): TransformerOutput = {
    val rdds = deps.collect {
      case data: DatasetOutput => data
    }
    require(rdds.size == deps.size)

    // TransformerOutput is constructed as call-by-name,
    // meaning fitRDDs will only be called when the transformer is used
    new TransformerOutput(fitRDDs(rdds))
  }
}

/**
 * This Operator is used to apply the output of an [[EstimatorOperator]] to new data.
 * It takes a [[TransformerOutput]] as its first dependency, accesses the contained
 * [[TransformerOperator]], and delegates the rest of its dependencies to that Transformer.
 *
 * The operator is lazy, meaning that nothing will be executed until the
 * value of the [[Output]] is accessed using `Output.get`.
 */
private[internals] class DelegatingOperator extends Operator with Serializable {
  override def execute(deps: Seq[Output]): Output = {
    val transformer = deps.collectFirst {
      case t: TransformerOutput => t
    }.get

    val depsAsRDD = deps.tail.collect {
      case data: DatasetOutput => data
    }

    val depsAsDatum = deps.tail.collect {
      case datum: DatumOutput => datum
    }

    if (depsAsRDD.nonEmpty) {
      // DatasetOutput is constructed as call-by-name,
      // meaning bulktransform will only be called when the output is used
      new DatasetOutput(transformer.get.batchTransform(depsAsRDD))
    } else {
      // DatumOutput is constructed as call-by-name,
      // meaning singleTransform will only be called when the output is used
      new DatumOutput(transformer.get.singleTransform(depsAsDatum))
    }
  }
}
