package workflow

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

private[workflow] case class GatherTransformer[T]() extends TransformerNode {
  def transform(dataDependencies: Seq[_]): Seq[T] = dataDependencies.map(_.asInstanceOf[T])

  def transformRDD(dataDependencies: Seq[RDD[_]]): RDD[Seq[T]] = {
    dataDependencies.map(_.asInstanceOf[RDD[T]].map(t => Seq(t))).reduceLeft((x, y) => {
      x.zip(y).map(z => z._1 ++ z._2)
    })
  }
}