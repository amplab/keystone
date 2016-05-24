package workflow.graph

import org.apache.spark.rdd.RDD
import pipelines.Logging

import scala.reflect.ClassTag

/**
 * Caches an RDD at a given point within a Pipeline. Follows Spark's lazy evaluation conventions.
 *
 * @param name An optional name to set on the cached output. Useful for debugging.
 * @tparam T Type of the input to cache.
 */
case class Cacher[T: ClassTag](name: Option[String] = None) extends Transformer[T,T] with Logging {
  override protected def batchTransform(in: RDD[T]): RDD[T] = {
    logInfo(s"CACHING ${name.getOrElse(in.id)}")
    name match {
      case Some(x) => in.cache().setName(x)
      case None => in.cache()
    }
  }

  override protected def singleTransform(in: T): T = in
}
