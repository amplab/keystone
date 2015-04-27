package nodes.nlp

import java.util.Locale

import org.apache.spark.rdd.RDD
import pipelines.Transformer

case class Tokenizer(sep: String = "[\\p{Punct}\\s]+") extends Transformer[String, Seq[String]] {
 override def apply(in: RDD[String]): RDD[Seq[String]] = in.map(_.split(sep))
}

/**
 * Transformer that trims a String
 */
object Trim extends Transformer[String, String] {
  override def apply(in: RDD[String]): RDD[String] = in.map(_.trim)
}

/**
 * Transformer that converts a String to lower case
 */
case class LowerCase(locale: Locale = Locale.getDefault) extends Transformer[String, String] {
  override def apply(in: RDD[String]): RDD[String] = in.map(_.toLowerCase(locale))
}