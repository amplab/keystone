package nodes.utils

import org.apache.spark.rdd.RDD
import pipelines.Pipelines.PipelineNode
import pipelines.{Logging, Transformer}
import utils._

/**
 * Caches the intermediate state of a node. Follows Spark's lazy evaluation conventions.
 * @param name An optional name to set on the cached output. Useful for debugging.
 * @tparam T Type of the input to cache.
 */
class Cacher[T](name: Option[String] = None) extends Transformer[T,T] with Logging {
  def apply(in: RDD[T]): RDD[T] = {
    logInfo(s"CACHING ${in.id}")
    name match {
      case Some(x) => in.cache().setName(x)
      case None => in.cache()
    }
  }
}

/**
 * Helper objects with constructors to create Cachers inline.
 */
object Cacher {
  def apply[T](): Cacher[T] = new Cacher[T]()
  def apply[T](name: String): Cacher[T] = new Cacher(Some(name))
}