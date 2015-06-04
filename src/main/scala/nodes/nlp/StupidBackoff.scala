package nodes.nlp

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD

import java.util.{HashMap => JHashMap}

import workflow.{Transformer, Estimator}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.reflect.ClassTag

/**
 * Partitions each ngram by hashing on its first two words (first as in farthest away
 * from the current word), then mod by `numPartitions`.
 *
 * Useful for grouping ngrams that share the first two words in context.  An example
 * usage is the [[StupidBackoffEstimator]].
 *
 * @param numPartitions the desired number of partitions
 * @param indexer a [[BackoffIndexer]]; required for retrieving first two words of ngrams
 * @tparam WordType type of each word (e.g. Int or String)
 */
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

private[nlp] object StupidBackoff {

  @tailrec def scoreLocally[T]
      (indexer: BackoffIndexer[T, NGram[T]],
       unigramCounts: collection.Map[T, Int],
       getNGramCount: NGram[T] => Int,
       numTokens: Int,
       alpha: Double = 0.4)
      (accum: Double,
       ngram: NGram[T],
       ngramFreq: Int): Double = {

    val ngramOrder = indexer.ngramOrder(ngram)
    if (ngramOrder == 1) {
      accum * ngramFreq / numTokens
    } else {
      if (ngramFreq != 0) {
        val context = indexer.removeCurrentWord(ngram)
        val contextFreq =
          if (ngramOrder != 2) getNGramCount(context)
          else unigramCounts.getOrElse(indexer.unpack(context, 0), 0)
        accum * ngramFreq / contextFreq
      } else {
        // This branch is only reached for out-of-corpus ngrams
        val backoffed = indexer.removeFarthestWord(ngram)
        val freq =
          if (ngramOrder != 2) getNGramCount(backoffed)
          else unigramCounts.getOrElse(indexer.unpack(backoffed, 0), 0)
        scoreLocally(indexer, unigramCounts, getNGramCount, numTokens, alpha)(
          alpha * accum, backoffed, freq)
      }
    }
  }

}

class StupidBackoffModel[@specialized(Int) T: ClassTag](
    val scoresRDD: RDD[(NGram[T], Double)],
    val ngramCounts: RDD[(NGram[T], Int)],
    indexer: BackoffIndexer[T, NGram[T]],
    val unigramCounts: collection.Map[T, Int],
    val numTokens: Int,
    val alpha: Double = 0.4) extends Transformer[(NGram[T], Int), (NGram[T], Double)] {

  // RDD lookup(); more efficient model serving is left for future work (e.g. IndexedRDD).
  private[this] val ngramLookup: NGram[T] => Int = { ngram =>
    val seq = ngramCounts.lookup(ngram)
    seq.length match {
      case 0 => 0 // unseen
      case cnt => seq(0) // by contract, `ngram` appears at most once in `ngramCounts`
    }
  }

  private[this] val scoreFunc = StupidBackoff.scoreLocally(
    indexer, unigramCounts, ngramLookup, numTokens, alpha) _

  def score(ngram: NGram[T]): Double =
    scoreFunc(1.0, ngram, ngramLookup(ngram))

  override def apply(ignored: RDD[(NGram[T], Int)]): RDD[(NGram[T], Double)] =
    throw new UnsupportedOperationException(
      "Doesn't make sense to chain this node; use method score(ngram) to query the model.")

  def apply(ignored: (NGram[T], Int)): (NGram[T], Double) =
    throw new UnsupportedOperationException(
      "Doesn't make sense to chain this node; use method score(ngram) to query the model.")

}

/**
 * Estimates a Stupid Backoff ngram language model, which was introduced in the
 * following paper:
 *
 *     Brants, Thorsten, et al. "Large language models in machine translation." 2007.
 *
 * The results are scores indicating likeliness of each ngram, but they are not normalized
 * probabilities.  The score for an n-gram is defined recursively:
 *
 *    S(w_i | w_{i - n + 1}^{i - 1}) :=
 *      if numerator > 0:  freq(w_{i - n + 1}^i) / freq(w_{i - n + 1}^{i - 1})
 *      otherwise:         \alpha * S(w_i | w_{i - n + 2}^{i - 1})
 *
 *    S(w_i) := freq(w_i) / N,  where N is the total number of tokens in training corpus.
 *
 * @param unigramCounts the pre-computed unigram counts of the training corpus
 * @param alpha hyperparameter that gets multiplied once per backoff
 */
case class StupidBackoffEstimator[@specialized(Int) T: ClassTag](
    unigramCounts: collection.Map[T, Int],
    alpha: Double = 0.4)
  extends Estimator[(NGram[T], Int), (NGram[T], Double)] {

  private[this] val indexer = new NGramIndexerImpl[T]
  private[this] lazy val numTokens = unigramCounts.values.sum

  def fit(data: RDD[(NGram[T], Int)]): StupidBackoffModel[T] = {
    val partitioned = data.reduceByKey(
      new InitialBigramPartitioner(data.partitions.length, indexer),
      _ + _
    )
    val scoresRDD = partitioned
      .mapPartitions(f = { part =>
        val ngramCountsMap = new JHashMap[NGram[T], Int]().asScala
        part.foreach { case (ngram, count) => ngramCountsMap.put(ngram, count) }

        val getNGramCount = { ngram: NGram[T] => ngramCountsMap.getOrElse(ngram, 0) }
        val scoreFunc = StupidBackoff.scoreLocally(
          indexer, unigramCounts, getNGramCount, numTokens, alpha) _

        ngramCountsMap.iterator.map { case (ngram, ngramFreq) =>
          val score = scoreFunc(1.0, ngram, ngramFreq)
          require(score >= 0.0 && score <= 1.0,
            f"""score = $score%.4f not in [0,1], ngram = $ngram
               |unigramCounts: ${unigramCounts.toSeq.mkString(",")}
             """.stripMargin)
          (ngram, score)
        }
      }, preservesPartitioning =  true)

    new StupidBackoffModel[T](scoresRDD, partitioned, indexer, unigramCounts, numTokens, alpha)
  }

}
