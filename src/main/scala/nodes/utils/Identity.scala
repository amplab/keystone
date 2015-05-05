import org.apache.spark.rdd.RDD
import pipelines.Transformer

/**
 * This class performs a no-op on its input.
 *
 * @tparam T Type of the input and, by definition, output.
 */
class Identity[T] extends Transformer[T,T] {
  def apply(in: RDD[T]): RDD[T] = in
}