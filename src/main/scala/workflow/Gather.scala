package workflow

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class GatherTransformer[T] extends TransformerNode[Seq[T]] {
  def transform(dataDependencies: Seq[_], fitDependencies: Seq[TransformerNode[_]]): Seq[T] = dataDependencies.map(_.asInstanceOf[T])

  def transformRDD(dataDependencies: Seq[RDD[_]], fitDependencies: Seq[TransformerNode[_]]): RDD[Seq[T]] = {
    dataDependencies.map(_.asInstanceOf[RDD[T]].map(t => Seq(t))).reduceLeft((x, y) => {
      x.zip(y).map(z => z._1 ++ z._2)
    })
  }
}

object Gather {
  def apply[A, B : ClassTag](branches: Seq[Pipeline[A, B]]): Pipeline[A, Seq[B]] = {
    branches.scanLeft(0)(_ + _.nodes.size).zip(branches)
    null
  }
}
