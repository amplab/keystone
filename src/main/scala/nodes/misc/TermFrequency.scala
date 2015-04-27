package nodes.misc

import org.apache.spark.rdd.RDD
import pipelines.Transformer

case class TermFrequency(fun: Double => Double = identity) extends Transformer[Seq[Any], Seq[(Any, Double)]] {
  override def apply(in: RDD[Seq[Any]]): RDD[Seq[(Any, Double)]] = in.map(_.groupBy(identity).mapValues(x => fun(x.size)).toSeq)
}