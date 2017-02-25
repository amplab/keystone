package keystoneml.loaders

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * A case class containing an RDD of labeled data
 * @tparam Label  The type of the labels
 * @tparam Datum  The type of the data
 */
case class LabeledData[Label : ClassTag, Datum : ClassTag](labeledData: RDD[(Label, Datum)]) {
  val data: RDD[Datum] = labeledData.map(_._2)
  val labels: RDD[Label] = labeledData.map(_._1)
}
