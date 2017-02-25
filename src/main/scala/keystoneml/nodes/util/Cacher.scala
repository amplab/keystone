package keystoneml.nodes.util

import org.apache.spark.rdd.RDD
import keystoneml.pipelines.Logging
import keystoneml.workflow.Transformer

import scala.reflect.ClassTag

/**
 * Caches an RDD at a given point within a Pipeline. Follows Spark's lazy keystoneml.evaluation conventions.
 *
 * @param name An optional name to set on the cached output. Useful for debugging.
 * @tparam T Type of the input to cache.
 */
case class Cacher[T: ClassTag](name: Option[String] = None) extends Transformer[T,T] with Logging {
  override def apply(in: RDD[T]): RDD[T] = {
    logInfo(s"CACHING ${name.getOrElse(in.id)}")
    name match {
      case Some(x) => in.cache().setName(x)
      case None => in.cache()
    }
  }

  override def apply(in: T): T = in
}
