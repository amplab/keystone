package nodes.nlp

import pipelines.nlp.{NGramIndexerImpl, BackoffIndexer}
import pipelines.Transformer

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD

import java.util.{HashMap => JHashMap}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.reflect.ClassTag

/** Useful for Stupid Backoff. */
class InitialBigramPartitioner[WordType: ClassTag](
    val numPartitions: Int,
    indexer: BackoffIndexer[WordType, NGram[WordType]]) extends Partitioner {

  /* Calculates 'x' modulo 'mod', takes to consideration sign of x,
   * i.e. if 'x' is negative, than 'x' % 'mod' is negative too
   * so function return (x % mod) + mod in that case.
   */
  private[this] def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }

  def getPartition(key: Any): Int = key match {
    case null => 0
    case ngramID: NGram[WordType] =>
      indexer.ngramOrder(ngramID) match {
        case x if x > 1 =>
          val firstWord = indexer.unpack(ngramID, 0)
          val secondWord = indexer.unpack(ngramID, 1)
          nonNegativeMod((firstWord, secondWord).hashCode(), numPartitions)
        case _ => 0
      }
    case _ => 0
  }

  override def equals(other: Any): Boolean = other match {
    case h: InitialBigramPartitioner[_] => h.numPartitions == numPartitions
    case _ => false
  }

  override def hashCode: Int = numPartitions

}

class StupidBackoffLM[@specialized(Int) T: ClassTag](
    val unigramCounts: scala.collection.Map[T, Int])
  extends Transformer[(NGram[T], Int), (NGram[T], Double)] {

  private[this] val indexer = new NGramIndexerImpl[T]

  lazy val numTokens = unigramCounts.values.sum

  @tailrec private[this] def stupidBackoffLocally(
       accum: Double,
       ngramId: indexer.NgramType,
       ngramFreq: Int,
       counts: mutable.Map[indexer.NgramType, Int],
       totalTokens: Int,
       unigrams: scala.collection.Map[indexer.WordType, Int],
       alpha: Double = 0.4): Double = {
    // TODO: this can be an arg, since it's mono. decreasing across recursive calls.
    val ngramOrder = indexer.ngramOrder(ngramId)
    if (ngramOrder == 1) {
      accum * ngramFreq / totalTokens
    } else {
      if (ngramFreq != 0) {
        val context = indexer.removeCurrentWord(ngramId)
        val contextFreq =
          if (ngramOrder != 2) counts.getOrElse(context, 0)
          else unigrams.getOrElse(indexer.unpack(context, 0), 0)
        accum * ngramFreq / contextFreq
      } else {
        val backoffed = indexer.removeFarthestWord(ngramId)
        val freq =
          if (ngramOrder != 2) counts.getOrElse(backoffed, 0)
          else unigrams.getOrElse(indexer.unpack(backoffed, 0), 0)
        stupidBackoffLocally(alpha * accum, backoffed, freq, counts, totalTokens, unigrams)
      }
    }
  }

  def apply(data: RDD[(NGram[T], Int)]): RDD[(NGram[T], Double)] = {
    val partitioned = data.reduceByKey(
      new InitialBigramPartitioner(data.partitions.length, indexer),
      _ + _
    )

    partitioned
      .mapPartitions(f = { part =>
        val ngramCountsMap = new JHashMap[NGram[T], Int]().asScala
        part.foreach { case (ngram, count) => ngramCountsMap.put(ngram, count) }

        ngramCountsMap.iterator.map { case (ngram, ngramFreq) =>
          val score = stupidBackoffLocally(
            1.0, ngram, ngramFreq, ngramCountsMap, numTokens, unigramCounts)
          require(score >= 0.0 && score <= 1.0, f"score = $score%.4f not in [0,1], ngram = $ngram")
          (ngram, score)
        }
      }, preservesPartitioning =  true)
  }

}
