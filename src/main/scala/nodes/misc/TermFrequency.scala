package nodes.misc

import org.apache.spark.rdd.RDD
import pipelines.Transformer

/**
 * Transformer that maps a Seq[Any] of objects to a Seq[(Any, Double)] of (unique object, weighting_scheme(tf)),
 * where tf is the number of times the unique object appeared in the original Seq[Any],
 * and the weighting_scheme is a lambda of Double => Double that defaults to the identity function.
 *
 * As an example, the following would return a transformer that maps a Seq[Any]
 * to all objects seen with the log of their count plus 1:
 * {{{
 *   TermFrequency(x => math.log(x) + 1)
 * }}}
 *
 * @param fun the weighting scheme to apply to the frequencies (defaults to identity)
 */
case class TermFrequency(fun: Double => Double = identity) extends Transformer[Seq[Any], Seq[(Any, Double)]] {
  override def apply(in: Seq[Any]): Seq[(Any, Double)] = in.groupBy(identity).mapValues(x => fun(x.size)).toSeq
}