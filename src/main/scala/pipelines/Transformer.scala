package pipelines

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag


/**
 * Transformers extend PipelineNode[RDD[A], RDD[B]] and exist just to reduce a bit of implementation boilerplate
 * @tparam Input input type of the transformer
 * @tparam Output output type of the transformer
 */
abstract class Transformer[Input, Output] extends PipelineNode[RDD[Input], RDD[Output]]

object Transformer {
  /**
   * This constructor takes a function and returns a Transformer that maps it over the input RDD
   * @param f The function to apply to every item in the RDD being transformed
   * @tparam I input type of the transformer
   * @tparam O output type of the transformer
   * @return Transformer that applies the given function to all items in the RDD
   */
  def apply[I, O : ClassTag](f: I => O): Transformer[I, O] = new Transformer[I, O] {
    override def apply(v1: RDD[I]): RDD[O] = v1.map(f)
  }
}
