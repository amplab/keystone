package keystoneml.workflow

import org.apache.spark.rdd.RDD

object WorkflowUtils {
  /**
    * Return the number of items in each partition in an RDD.
    * @param rdd Input RDD.
    * @tparam T RDD Type.
    * @return A [[Map]] keyed by partition ID containing the number of elements in each partition of the RDD.
    */
  def numPerPartition[T](rdd: RDD[T]): Map[Int, Int] = {
    rdd.mapPartitionsWithIndex {
      case (id, partition) => Iterator.single((id, partition.length))
    }.collect().toMap
  }

}