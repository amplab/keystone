package workflow.old

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * This class performs a no-op on its input.
 *
 * @tparam T Type of the input and, by definition, output.
 */
class Identity[T: ClassTag] extends Transformer[T,T] {
  def apply(in: T): T = in
  override def apply(in: RDD[T]): RDD[T] = in
}