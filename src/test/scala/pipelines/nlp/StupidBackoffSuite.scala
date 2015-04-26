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

  def featurizer(orders: Seq[Int], mode: String = "default") = SimpleTokenizer then
    new NGramsFeaturizer[String](orders) then
    new NGramsCounts[String](mode)

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

  test("end to end InitialBigramPartitioner") {
    sc = new SparkContext("local[4]", "StupidBackoffSuite")
    val corpus = sc.parallelize(data, 3)
    val ngrams = featurizer(2 to 5, "noAdd")(corpus)
    val unigrams = featurizer(1 to 1)(corpus)
      .collectAsMap()
      .map { case (key, value) => key.words(0) -> value }

    val stupidBackoff = new StupidBackoffLM[String](unigrams)
    requireNGramColocation(stupidBackoff(ngrams), new NGramIndexerImpl[String])
  }

}
