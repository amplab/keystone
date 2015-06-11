package workflow

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Created by tomerk11 on 5/28/15.
 */
case class PipelineModel[A, B : ClassTag] private[workflow] (transformers: Seq[Transformer[_, _]]) extends Transformer[A, B] {
  def apply(in: A): B = {
    var x: Any = in
    transformers.foreach { transformer =>
      x = transformer.unsafeSingleApply(x)
    }
    x.asInstanceOf[B]
  }

  override def apply(in: RDD[A]): RDD[B] = {
    var x: Any = in
    transformers.foreach { transformer =>
      x = transformer.unsafeRDDApply(x)
    }
    x.asInstanceOf[RDD[B]]
  }

  override def rewrite: Seq[Transformer[_, _]] = transformers.flatMap(_.rewrite)
}
