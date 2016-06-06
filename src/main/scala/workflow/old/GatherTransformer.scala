package workflow

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

private[workflow] case class GatherTransformer[T]() extends TransformerNode {
  def transform(dataDependencies: Iterator[_]): Seq[T] = dataDependencies.map(_.asInstanceOf[T]).toSeq

  def transformRDD(dataDependencies: Iterator[RDD[_]]): RDD[Seq[T]] = {
    dataDependencies.map(_.asInstanceOf[RDD[T]].map(t => Seq(t))).reduceLeft((x, y) => {
      x.zip(y).map(z => z._1 ++ z._2)
    })
  }
}