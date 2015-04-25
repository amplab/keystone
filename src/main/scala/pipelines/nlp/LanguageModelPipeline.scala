package pipelines.nlp

import nodes.nlp.{NGram, SimpleTokenizer, NGramsCounts, NGramsFeaturizer}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

// TODO: take a look at http://berkeleylm.googlecode.com/svn/trunk/doc/edu/berkeley/nlp/lm/map/NgramMap.html

/**
 * Datasets to consider:
 *  - http://corpus.byu.edu/coca/
 *  - (list) http://www.bmanuel.org/clr/clr2_mp.html
 *  - Proceedings of Canadian Parliament (CS288)
 *  - LDC (https://catalog.ldc.upenn.edu/) - not free?
 *  - Project Gutenberg novels?
 *  - Common Crawl?
 */

object LanguageModelPipeline {

  val indexer = IntArrayIndexer

  def timed[A](f: => A): (A, Long) = {
    val begin = System.nanoTime()
    val res = f
    val end = System.nanoTime()
    (res, ((end - begin) / 1e6).toInt)
  }

  /** For debugging. */
  def asBinary(x: Long): String = {
    var n = x
    val sb = new mutable.StringBuilder()
    while (n != 0) {
      sb.append(n & 1)
      n = n >>> 1
    }
    val res = sb.toString().reverse
    ("0" * (64 - res.length)) + res
  }


  def requireNGramColocationLocally(index: Int, part: Iterator[(indexer.NgramType, Int)]) = {
    val map = new mutable.HashMap[indexer.NgramType, Int]()
    part.foreach { case (ngramId, count) => map.put(ngramId, count) }
    map.keySet.foreach { ngramId =>
      val order = indexer.ngramOrder(ngramId)
      order match {
        case 2 =>
          // TODO: unigrams separeate consideration?
//          require(map.contains(LanguageIndexer.depackBigram(ngramId)))
        case 3 =>
          val prev = indexer.unpack(ngramId, 1)
          val prevPrev = indexer.unpack(ngramId, 0)
          val bigramContext = indexer.pack(Seq(prevPrev, prev))
          require(map.contains(bigramContext),
            s"tri-gram is not co-located with its bigram context; ngramId = $ngramId, bigram = $bigramContext")

          // TODO: unigrams separeate consideration?
//          require(map.contains(prevPrev))
        case _ =>
      }
    }
  }

  def checkNGramColocation(ngramsCounts: RDD[(indexer.NgramType, Int)]) = {
    ngramsCounts.mapPartitionsWithIndex { (index, part) =>
      requireNGramColocationLocally(index, part)
      Iterator.empty
    }.count()
  }

  // TODO: combine counts, totalTokens, and unigrams into something like 'localLmHandle'?
  // TODO: use log space?
  @tailrec def stupidBackoffLocally(
      accum: Double,
      ngramId: indexer.NgramType,
      ngramFreq: Int,
      counts: mutable.Map[indexer.NgramType, Int],
      totalTokens: Int,
      unigrams: scala.collection.Map[indexer.WordType, Int],
      alpha: Double = 0.4)
  : Double = {
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

  final val ngramOrders = 1 to 5

  def main(args: Array[String]) {

    if (args.length < 2) {
      sys.error("usage: <sparkMaster> <trainData>")
    }
    val sparkMaster = args(0)
    val trainData = args(1)

    val conf = new SparkConf().setMaster(sparkMaster).setAppName("LanguageModelPipeline")
    val sc = new SparkContext(conf)

    val text = sc.textFile(trainData)

    /** Vocab generation step */
    // RDD[String] -> RDD[(WordType, Count)]; on the order of 200K items for 'target'
    var startTime = System.nanoTime()

    val extractUnigrams = SimpleTokenizer then
      new NGramsFeaturizer(1 to 1) then
      NGramsCounts
    val unigramCounts = extractUnigrams(text)

    var endTime = System.nanoTime()

    // Count total tokens via an Accumulator; the result will be captured by closures.
    val totalTokensAccumulator = sc.accumulator(0, "Total tokens in training data")
    unigramCounts.foreach { case (_, cnt) => totalTokensAccumulator += cnt }
    val totalTokens = totalTokensAccumulator.value
    println(s"total tokens = $totalTokens; unique tokens = ${unigramCounts.count()}")
    println(f"unigram counts generation took ${(endTime - startTime) / 1e6}%.1f millis")

    val wordIndexRDD = unigramCounts
      .zipWithIndex() // indexes respect the sorted order
      .map { case ((word, count), index) =>
        // The cast is valid if number of word types in training data is less than Int.MaxValue.
        (word, index.asInstanceOf[Int])
      }

//    wordIndexRDD.saveAsTextFile("./wordIndex")
//    wordIndexRDD.map(_.swap).saveAsTextFile("./indexToWord")

    // The WordString -> ID map is sent to each executor as a broadcast variable.
    val (wordIndex, duration) = timed { wordIndexRDD.collectAsMap() }
//    val wordIndexSize = utils.SizeEstimator.estimate(wordIndex) / 1024.0
//    println(f"word index: estimated size $wordIndexSize%.1f KB, collecting to driver took $duration millis")
    println(f"word index: collecting to driver took $duration millis")
    val wordIndexBroadcast = sc.broadcast(wordIndex)

    /** NGram generation step */

    // TODO: the communication path is "executors -> driver -> executors". It'd be better if Spark
    // supports say, TorrentBroadcast among executors without going thru. driver: "executors -> executors".

    // Unigram counts (Id -> Int) need to be replicated to all executors.
    // TODO: we can avoid reading broadcast val here (and doing hash map lookups) by sharing unigramCounts.zipWithIndex().
    val unigrams = unigramCounts.map { case (word, count) => (wordIndexBroadcast.value(word), count) }

//    unigrams.saveAsTextFile("./unigrams") // TODO: saved for debugging.

    val (materializedUnigrams, t) = timed { unigrams.collectAsMap() }
    // TODO: use reflection to call Spark?
//    val unigramsSize = utils.SizeEstimator.estimate(materializedUnigrams) / 1024.0
//    println(f"unigrams (long-indexed): estimated size $unigramsSize%.1f KB, collecting to driver took $t millis")
    println(f"unigrams (long-indexed): collecting to driver took $t millis")
    val unigramsBroadcast = sc.broadcast(materializedUnigrams)

    // Generate intermediate RDD[(NgramType, Count)], where n > 1.
    // TODO: heavy GC in this block! Easily 1min+ per task (< 5.5min total duration).
    startTime = System.nanoTime()

    val intermediateNGrams = text.mapPartitions { lines =>
      val counts = new java.util.HashMap[indexer.NgramType, Int]().asScala
      val buf = new ArrayBuffer[Int](ngramOrders.max)

      lines.foreach { line =>
        val tokenIds = line.trim.split(" ") // TODO: better tokenization
          .map(tok => wordIndexBroadcast.value(new NGram(Array(tok)))) // FIXME!!!
//          .map(tok => wordIndexBroadcast.value(tok))
        var i = 0
        while (i < tokenIds.length) {
          buf.clear() // Can still be optimized by cycling?
          buf += tokenIds(i)
          var order = 2
          // Separately consider unigrams, so not counting them here.
          while (order <= ngramOrders.max && i + order <= tokenIds.length) {
            buf += tokenIds(i + order - 1)
            val ngramId = indexer.pack(buf)
            val currCount = counts.getOrElse(ngramId, 0)
            counts.put(ngramId, currCount + 1)
            order += 1
          }
          i += 1
        }
      }
      counts.toIterator
    }

    // Reduce by key, while maintaining the "initial ngram co-location" property.
    println(s"intermediate partitions size: ${intermediateNGrams.partitions.size}")
    val ngramsCounts = intermediateNGrams.reduceByKey(
      new InitialBigramPartitioner(intermediateNGrams.partitions.size, indexer),
      _ + _)

    // TODO: should just call .count()?
    ngramsCounts.cache().count()
//    ngramsCounts.mapPartitions(p => Iterator(p.length)).count()

    endTime = System.nanoTime()
    println(f"Building NGram RDD and properly shuffling it took ${(endTime - startTime) / 1e6} millis")
    ngramsCounts.saveAsTextFile("./ngrams")

    checkNGramColocation(ngramsCounts)

    /** Language model (Stupid Backoff) generation step */

    startTime = System.nanoTime()
    val model = ngramsCounts
      .mapPartitions(f = { part =>
        val ngramCountsMap = new mutable.HashMap[indexer.NgramType, Int]()
        part.foreach { case (ngramId, count) => ngramCountsMap.put(ngramId, count) }
        ngramCountsMap.iterator.map { case (ngramId, ngramFreq) =>
          val score = stupidBackoffLocally(1.0, ngramId, ngramFreq, ngramCountsMap, totalTokens, unigramsBroadcast.value)
          require(score >= 0.0 && score <= 1.0,
            s"score = $score, ngramId = $ngramId")
          (ngramId, score)
        }
      }, preservesPartitioning =  true)
    println(s"num ngram (n > 1) types: ${model.count()}")
    println(s"num unigram types: ${materializedUnigrams.size}")
    endTime = System.nanoTime()
    println(f"Building SB scores RDD took ${(endTime - startTime) / 1e6}%.1f millis")

    model.saveAsTextFile("./model")

    /** Serving step */
    // FIXME: how to serve unigrams?

//    println(s"scores from normal RDD:")
//    println(s"score for 'pipelines' = ${model.lookup(wordIndex("pipelines").get)}")
//    println(s"score for 'Machine learning pipelines' = " +
//      model.lookup(LanguageIndexer.packToNGramID(Seq("Machine", "learning", "pipelines"), wordIndex)))
//    println(s"score for 'EC2' = ${model.lookup(wordIndex("EC2").get)}")

//    val indexedModel = IndexedRDD(model.asInstanceOf[RDD[(indexer.NgramType, Double)]]).cache()
//    val (_, indexingTime) = timed { indexedModel.count() }
//    println(s"creating IndexedRDD took $indexingTime millis")

//    println(s"scores from IndexedRDD:")
//    println(s"score for 'pipelines' = ${indexedModel.get(wordIndex("pipelines").get)}")
//    println(s"score for 'Machine learning pipelines' = " +
//      indexedModel.get(LanguageIndexer.packToNGramID(Seq("Machine", "learning", "pipelines"), wordIndex)))
//    println(s"score for 'EC2' = ${indexedModel.get(wordIndex("EC2").get)}")

    sc.stop()
  }

}
