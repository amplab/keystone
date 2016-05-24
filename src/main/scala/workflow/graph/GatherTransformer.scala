package workflow.graph

import org.apache.spark.rdd.RDD

/**
 * A [[TransformerOperator]] that gathers multiple datasets of {@tparam T} into a dataset of Seq[T]
 * (Or individual datums of T into a single Seq[T])
 */
private[workflow] case class GatherTransformer[T]() extends TransformerOperator {
  override private[graph] def singleTransform(inputs: Seq[DatumExpression]): Any = {
    inputs.map(_.get.asInstanceOf[T])
  }

  override private[graph] def batchTransform(inputs: Seq[DatasetExpression]): RDD[_] = {
    inputs.map(_.get.asInstanceOf[RDD[T]].map(t => Seq(t))).reduceLeft((x, y) => {
      x.zip(y).map(z => z._1 ++ z._2)
    })
  }
}