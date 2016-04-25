package internals

import org.apache.spark.rdd.RDD

sealed trait Operator {
  def label: String = {
    val className = getClass.getSimpleName
    if (className endsWith "$") className.dropRight(1) else className
  }

  def execute(deps: Seq[Output]): Output
}

private[internals] case class DatasetOperator(rdd: RDD[_]) extends Operator {
  override def label: String = "%s[%d]".format(
    Option(rdd.name).map(_ + " ").getOrElse(""), rdd.id)

  override def execute(deps: Seq[Output]): DatasetOutput = {
    new DatasetOutput(rdd)
  }
}

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

private[internals] abstract class TransformerOperator extends Operator with Serializable {
  private[internals] def singleTransform(dataDependencies: Seq[DatumOutput]): Any
  private[internals] def bulkTransform(dataDependencies: Seq[DatasetOutput]): RDD[_]

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
      new DatasetOutput(bulkTransform(depsAsRDD))
    } else {
      // DatumOutput is constructed as call-by-name,
      // meaning singleTransform will only be called when the output is used
      new DatumOutput(singleTransform(depsAsDatum))
    }
  }
}

private[internals] abstract class EstimatorOperator extends Operator with Serializable {
  def fitRDDs(in: Seq[DatasetOutput]): TransformerOperator

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
      new DatasetOutput(transformer.get.bulkTransform(depsAsRDD))
    } else {
      // DatumOutput is constructed as call-by-name,
      // meaning singleTransform will only be called when the output is used
      new DatumOutput(transformer.get.singleTransform(depsAsDatum))
    }
  }
}
