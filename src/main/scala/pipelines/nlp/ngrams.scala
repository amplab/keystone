package pipelines.nlp

import pipelines.Pipelines.Transformer

import org.apache.spark.rdd.RDD

import java.util.{HashMap => JHashMap}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Common ngram implementation shared by user-facing ones.
 * @param orders valid ngram orders, must be consecutive positive integers
 * @param makeNGram function to convert a sequence of string tokens into ngram representation
 * @tparam NGramType type of an ngram; needs to be well-behaved when put in a hash map
 */
private[pipelines] abstract class NGramsBase[NGramType]
  (orders: Seq[Int])(makeNGram: Seq[String] => NGramType)
  extends Transformer[String, (NGramType, Int)] {

  private[this] final val minOrder = orders.min
  private[this] final val maxOrder = orders.max

  require(minOrder >= 1, s"minimum order is not >= 1, found $minOrder")
  orders.sliding(2).foreach {
    case xs if xs.length > 1 => require(xs(0) == xs(1) - 1,
      s"orders are not consecutive; contains ${xs(0)} and ${xs(1)}")
    case _ =>
  }

  @inline
  private[this] def recordNGram(buf: ArrayBuffer[String], counts: mutable.Map[NGramType, Int]) = {
    val ngramId = makeNGram(buf)
    val currCount = counts.getOrElse(ngramId, 0)
    counts.put(ngramId, currCount + 1)
  }

  override def apply(in: RDD[String]): RDD[(NGramType, Int)] = {
    in.mapPartitions { lines =>
      val counts = new JHashMap[NGramType, Int]().asScala
      val buf = new ArrayBuffer[String](orders.max)
      var j = 0
      var order = 0

      lines.foreach { line =>
        var i = 0
        val tokens = line.trim.split(" ") // TODO: better tokenization
        while (i + minOrder <= tokens.length) {
          buf.clear()

          j = i
          while (j < i + minOrder) {
            buf += tokens(j)
            j += 1
          }
          recordNGram(buf, counts)

          order = minOrder + 1
          while (order <= maxOrder && i + order <= tokens.length) {
            buf += tokens(i + order - 1)
            recordNGram(buf, counts)
            order += 1
          }
          i += 1
        }
      }
      counts.toIterator
    }
  }

}

/**
 * An NGram representation that is a thin wrapper over Array[String].  The
 * underlying tokens can be accessed via `words`.
 *
 * Its `hashCode` and `equals` implementations are sane so that it can
 * be used as keys in RDDs or hash tables.
 */
class NGram(final val words: Array[String]) {
  private[this] final val PRIME = 31

  override def hashCode: Int = {
    var hc = PRIME
    var i = 0
    while (i < words.length) {
      hc = (hc + words(i).hashCode) * PRIME
      i += 1
    }
    hc
  }

  override def equals(that: Any): Boolean = that match {
    case arr: NGram =>
      if (words.length != arr.words.length) {
        false
      } else {
        var i = 0
        while (i < words.length) {
          if (!words(i).equals(arr.words(i))) {
            return false
          }
          i += 1
        }
        true
      }
    case _ => false
  }

  override def toString = s"[${words.mkString(",")}]"
}

/**
 * A simple n-gram counter that represents each token as a String and each
 * ngram as an [[NGram]] (thin wrapper over Array[String]).  This implementation
 * may not be GC-friendly or space-efficient, but should handle commonly-sized
 * workloads well.
 *
 * Returns an RDD[(NGram, Int)] that is sorted by frequency in descending order.
 *
 * @param orders valid ngram orders, must be consecutive positive integers
 */
class NGrams(orders: Seq[Int])
  extends NGramsBase[NGram](orders)(strings => new NGram(strings.toArray)) {

  override def apply(in: RDD[String]): RDD[(NGram, Int)] = {
    super.apply(in)
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
  }

}
