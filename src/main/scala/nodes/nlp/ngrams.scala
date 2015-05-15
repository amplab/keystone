package nodes.nlp

import pipelines.Transformer

import org.apache.spark.rdd.RDD

import java.util.{HashMap => JHashMap}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
 * An ngram featurizer.
 *
 * @param orders valid ngram orders, must be consecutive positive integers
 */
case class NGramsFeaturizer[@specialized(Int) T: ClassTag](orders: Seq[Int])
  extends Transformer[Seq[T], Seq[Seq[T]]] {

  private[this] final val minOrder = orders.min
  private[this] final val maxOrder = orders.max

  require(minOrder >= 1, s"minimum order is not >= 1, found $minOrder")
  orders.sliding(2).foreach {
    case xs if xs.length > 1 => require(xs(0) == xs(1) - 1,
      s"orders are not consecutive; contains ${xs(0)} and ${xs(1)}")
    case _ =>
  }

  override def apply(in: RDD[Seq[T]]): RDD[Seq[Seq[T]]] = {
    in.mapPartitions { lines =>
      val ngramBuf = new ArrayBuffer[T](orders.max)
      var j = 0
      var order = 0
      lines.map { tokens =>
        val ngramsBuf = new ArrayBuffer[Seq[T]]()
        var i = 0
        while (i + minOrder <= tokens.length) {
          ngramBuf.clear()

          j = i
          while (j < i + minOrder) {
            ngramBuf += tokens(j)
            j += 1
          }
          ngramsBuf += ngramBuf.clone()

          order = minOrder + 1
          while (order <= maxOrder && i + order <= tokens.length) {
            ngramBuf += tokens(i + order - 1)
            ngramsBuf += ngramBuf.clone()
            order += 1
          }
          i += 1
        }
        ngramsBuf
      }
    }
  }

  def apply(line: Seq[T]): Seq[Seq[T]] = {
    val ngramBuf = new ArrayBuffer[T](orders.max)
    var j = 0
    var order = 0
    val ngramsBuf = new ArrayBuffer[Seq[T]]()
    var i = 0
    while (i + minOrder <= line.length) {
      ngramBuf.clear()

      j = i
      while (j < i + minOrder) {
        ngramBuf += line(j)
        j += 1
      }
      ngramsBuf += ngramBuf.clone()

      order = minOrder + 1
      while (order <= maxOrder && i + order <= line.length) {
        ngramBuf += line(i + order - 1)
        ngramsBuf += ngramBuf.clone()
        order += 1
      }
      i += 1
    }
    ngramsBuf
  }

}

/**
 * An NGram representation that is a thin wrapper over Array[String].  The
 * underlying tokens can be accessed via `words`.
 *
 * Its `hashCode` and `equals` implementations are sane so that it can
 * be used as keys in RDDs or hash tables.
 */
class NGram[@specialized(Int) T: ClassTag](final val words: Seq[T]) extends Serializable {
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
    case arr: NGram[T] =>
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
 * Control flags used for [[NGramsCounts]].  Use `Default` if counts are to be
 * aggregated across partitions and sorted; use `NoAdd` if just count ngrams within
 * each partition, with no cross-partition summing or sorting.
 */
object NGramsCountsMode extends Enumeration {
  val Default, NoAdd = Value
}

/**
 * A simple transformer that represents each ngram as an [[NGram]] and counts
 * their occurrence.  Returns an RDD[(NGram, Int)] that is sorted by frequency
 * in descending order.
 *
 * This implementation may not be space-efficient, but should handle commonly-sized
 * workloads well.
 *
 * @param mode a control flag defined in [[NGramsCountsMode]]
 */
case class NGramsCounts[T: ClassTag](mode: NGramsCountsMode.Value = NGramsCountsMode.Default)
  extends Transformer[Seq[Seq[T]], (NGram[T], Int)] {

  // Output uses NGram as key type, since aggregation of counts uses a hash map,
  // and we need the ngram representation to have sane .equals() and .hashCode().
  override def apply(rdd: RDD[Seq[Seq[T]]]): RDD[(NGram[T], Int)] = {
    val ngramCnts = rdd.mapPartitions { lines =>
      val counts = new JHashMap[NGram[T], Int]().asScala
      var i = 0
      var ngram: NGram[T] = null
      while (lines.hasNext) {
        val line = lines.next()
        i = 0
        while (i < line.length) {
          ngram = new NGram[T](line(i))
          val currCount = counts.getOrElse(ngram, 0)
          counts.put(ngram, currCount + 1)
          i += 1
        }
      }
      counts.toIterator
    }

    mode match {
      case NGramsCountsMode.Default => ngramCnts
        .reduceByKey(_ + _).sortBy(_._2, ascending = false)
      case NGramsCountsMode.NoAdd => ngramCnts
      case _ => throw new IllegalArgumentException(s"`mode` must be `default` or `noAdd`")
    }
  }

  def apply(line: Seq[Seq[T]]): (NGram[T], Int) = throw new UnsupportedOperationException(
      "Doesn't make sense to call single-item apply() on this node")

}
