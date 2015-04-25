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
class NGramsFeaturizer(orders: Seq[Int]) extends Transformer[Seq[String], Seq[Seq[String]]] {

  private[this] final val minOrder = orders.min
  private[this] final val maxOrder = orders.max

  require(minOrder >= 1, s"minimum order is not >= 1, found $minOrder")
  orders.sliding(2).foreach {
    case xs if xs.length > 1 => require(xs(0) == xs(1) - 1,
      s"orders are not consecutive; contains ${xs(0)} and ${xs(1)}")
    case _ =>
  }

  override def apply(in: RDD[Seq[String]]): RDD[Seq[Seq[String]]] = {
    in.mapPartitions { lines =>
      val ngramsBuf = new ArrayBuffer[Seq[String]]()
      val ngramBuf = new ArrayBuffer[String](orders.max)
      var j = 0
      var order = 0
      lines.foreach { tokens =>
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
      }
      Iterator.single(ngramsBuf)
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
class NGram[@specialized(Int) T: ClassTag](final val words: Array[T])
  extends Serializable {

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
 * A simple transformer that represents each ngram to an [[NGram]] and counts
 * their occurance.  Returns an RDD[(NGram, Int)] that is sorted by frequency
 * in descending order.
 *
 * This implementation may not be space-efficient, but should handle commonly-sized
 * workloads well.
 *
 * @param mode "default": aggregated and sorted; "noAdd": just count within partitions.
 */
class NGramsCounts[T: ClassTag](mode: String = "default")
  extends Transformer[Seq[Seq[String]], (NGram[T], Int)] {

  override def apply(rdd: RDD[Seq[Seq[String]]]): RDD[(NGram, Int)] = {
    val ngramCnts = rdd.mapPartitions { lines =>
      val counts = new JHashMap[NGram, Int]().asScala
      var i = 0
      var ngram: NGram = null
      while (lines.hasNext) {
        val line = lines.next()
        i = 0
        while (i < line.length) {
          ngram = new NGram[T](line(i).toArray)
          val currCount = counts.getOrElse(ngram, 0)
          counts.put(ngram, currCount + 1)
          i += 1
        }
      }
      counts.toIterator
    }
    mode match {
      case "default" => ngramCnts.reduceByKey(_ + _).sortBy(_._2, ascending = false)
      case "noAdd" => ngramCnts
      case _ => throw new IllegalArgumentException(s"`mode` must be `default` or `noAdd`")
    }

  }
