package workflow

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

private[workflow] class GatherTransformer[T] extends TransformerNode[Seq[T]] {
  def transform(dataDependencies: Seq[_], fitDependencies: Seq[TransformerNode[_]]): Seq[T] = dataDependencies.map(_.asInstanceOf[T])

  def transformRDD(dataDependencies: Seq[RDD[_]], fitDependencies: Seq[TransformerNode[_]]): RDD[Seq[T]] = {
    dataDependencies.map(_.asInstanceOf[RDD[T]].map(t => Seq(t))).reduceLeft((x, y) => {
      x.zip(y).map(z => z._1 ++ z._2)
    })
  }
}

object Gather {
  def apply[A, B : ClassTag](branches: Seq[Pipeline[A, B]]): Pipeline[A, Seq[B]] = {
    // attach a value per branch to offset all existing node ids by.
    val branchesWithNodeOffsets = branches.scanLeft(0)(_ + _.nodes.size).zip(branches)

    val newNodes = branches.map(_.nodes).reduceLeft(_ ++ _) :+ new GatherTransformer[B]

    val newDataDeps = branchesWithNodeOffsets.map { case (offset, branch) =>
      val dataDeps = branch.dataDeps
      dataDeps.map(_.map(x => if (x == Pipeline.SOURCE) Pipeline.SOURCE else x + offset))
    }.reduceLeft(_ ++ _) :+  branchesWithNodeOffsets.map { case (offset, branch) =>
      val sink = branch.sink
      if (sink == Pipeline.SOURCE) Pipeline.SOURCE else sink + offset
    }

    val newFitDeps = branchesWithNodeOffsets.map { case (offset, branch) =>
      val fitDeps = branch.fitDeps
      fitDeps.map(_.map(x => if (x == Pipeline.SOURCE) Pipeline.SOURCE else x + offset))
    }.reduceLeft(_ ++ _) :+  Seq()

    val newSink = newNodes.size - 1
    Pipeline(newNodes, newDataDeps, newFitDeps, newSink)
  }
}
