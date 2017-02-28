package keystoneml.nodes.util

import org.apache.spark.rdd.RDD
import keystoneml.pipelines.Logging
import keystoneml.workflow.Transformer

import scala.reflect.ClassTag

/**
 * Randomly shuffle the rows of an RDD within a pipeline. Uses a shuffle operation in Spark.
 *
 * @param numParts An optional parameter indicating the number of output partitions. 
 * @tparam T Type of the input to shuffle.
 */
class Shuffler[T: ClassTag](numParts: Option[Int] = None) extends Transformer[T,T] with Logging {
  override def apply(in: RDD[T]): RDD[T] = {
    val numToRepartition = numParts.getOrElse(in.partitions.size)
    in.repartition(numToRepartition)
  }

  override def apply(in: T): T = in
}
