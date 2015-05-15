package pipelines.nlp

import nodes.nlp._
import pipelines.LocalSparkContext

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import org.scalatest.FunSuite

import scala.collection.JavaConverters._

class StupidBackoffSuite extends FunSuite with LocalSparkContext {

  val data = Seq("Winter is coming",
    "Finals are coming",
    "Summer is coming really soon")

  def featurizer(orders: Seq[Int], mode: NGramsCountsMode.Value = NGramsCountsMode.Default) =
    Tokenizer() then
      NGramsFeaturizer[String](orders) then
      NGramsCounts[String](mode)

  def requireNGramColocation[T, V](
      ngrams: RDD[(NGram[T], V)],
      indexer: BackoffIndexer[T, NGram[T]]) = {

    ngrams.mapPartitions { part =>
      val map = new java.util.HashMap[NGram[T], V]().asScala
      part.foreach { case (ngramId, count) => map.put(ngramId, count) }

      map.keySet.foreach { ngramId =>
        var currNGram = ngramId
        while (indexer.ngramOrder(currNGram) > 2) {
          val context = indexer.removeCurrentWord(currNGram)
          require(map.contains(context),
            s"ngram $currNGram is not co-located with its context $context within same partition")
          currNGram = context
        }
      }
      Iterator.empty
    }.count()
  }

  test("end-to-end InitialBigramPartitioner") {
    sc = new SparkContext("local[4]", "StupidBackoffSuite")
    val corpus = sc.parallelize(data, 3)
    val ngrams = featurizer(2 to 5, NGramsCountsMode.NoAdd)(corpus)
    val unigrams = featurizer(1 to 1)(corpus)
      .collectAsMap()
      .map { case (key, value) => key.words(0) -> value }

    val stupidBackoff = StupidBackoffEstimator[String](unigrams).fit(ngrams)
    requireNGramColocation[String, Double](stupidBackoff.scoresRDD, new NGramIndexerImpl)
  }

  test("Stupid Backoff calculates correct scores") {
    sc = new SparkContext("local[4]", "StupidBackoffSuite")
    val corpus = sc.parallelize(data, 3)
    val ngrams = featurizer(2 to 5, NGramsCountsMode.NoAdd)(corpus)
    val unigrams = featurizer(1 to 1)(corpus)
      .collectAsMap()
      .map { case (key, value) => key.words(0) -> value }
    val lm = StupidBackoffEstimator[String](unigrams).fit(ngrams)

    assert(lm.score(new NGram(Seq("is", "coming"))) === 2.0 / 2.0)
    assert(lm.score(new NGram(Seq("is", "coming", "really"))) === 1.0 / 2.0)

    assert(lm.score(new NGram(Seq("is", "unseen-coming"))) === 0,
      "not equal to expected: bacoffed once & curr word unseen, so should be zero")
    assert(lm.score(new NGram(Seq("is-unseen", "coming"))) === lm.alpha * 3.0 / lm.numTokens,
      "not equal to expected: backoffed once, should be alpha * currWordCount / numTokens")
  }

}
