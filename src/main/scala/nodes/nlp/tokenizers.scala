package nodes.nlp

import org.apache.spark.rdd.RDD
import pipelines.Transformer

object SimpleTokenizer extends Transformer[String, Seq[String]] {

  override def apply(in: RDD[String]): RDD[Seq[String]] =
    in.map(_.trim.split(" "))

}
