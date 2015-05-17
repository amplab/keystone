package nodes.nlp

import java.util.Locale

import org.apache.spark.rdd.RDD
import pipelines.Transformer

/**
 * Transformer that tokenizes a String into a Seq[String] by splitting on a regular expression.
 * @param sep the delimiting regular expression to split on.
 *            Defaults to matching all punctuation and whitespace
 */
case class Tokenizer(sep: String = "[\\p{Punct}\\s]+") extends Transformer[String, Seq[String]] {
  override def apply(in: String): Seq[String] = in.split(sep)
}

/**
 * Transformer that trims a String of leading and trailing whitespace
 */
object Trim extends Transformer[String, String] {
  override def apply(in: String): String = in.trim
}

/**
 * Transformer that converts a String to lower case
 * @param locale  The locale to use. Defaults to `Locale.getDefault`
 */
case class LowerCase(locale: Locale = Locale.getDefault) extends Transformer[String, String] {
  override def apply(in: String): String = in.toLowerCase(locale)
}