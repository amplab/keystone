package pipelines.nlp

import pipelines.LocalSparkContext
import pipelines.Pipelines._

import org.apache.spark.SparkContext

import org.scalatest._

class NGramSuite extends FunSuite with LocalSparkContext {

  val tokenizer = Transformer { x: String => x.split(" ").toSeq }

  test("NGramsFeaturizer") {
    sc = new SparkContext("local[2]", "NGramSuite")
    val rdd = sc.parallelize(Seq("Pipelines are awesome", "NLP is awesome"), 2)

    def run(orders: Seq[Int]) = {
      val pipeline = tokenizer then
        new NGramsFeaturizer(orders)

      pipeline(rdd)
        .mapPartitions(_.flatten) // for comparison
        .collect()
        .toSeq.map(_.toSeq) // for comparison
    }

    val unigrams = Seq(
      Seq("Pipelines"), Seq("are"), Seq("awesome"),
      Seq("NLP"), Seq("is"), Seq("awesome")
    )
    assert(run(Seq(1)) === unigrams)

    val bigramTrigrams = Seq(
      Seq("Pipelines", "are"), Seq("Pipelines", "are", "awesome"), Seq("are", "awesome"),
      Seq("NLP", "is"), Seq("NLP", "is", "awesome"), Seq("is", "awesome")
    )
    assert(run(2 to 3) === bigramTrigrams)

    assert(run(Seq(6)) === Seq.empty, "returns 6-grams when there aren't any")
  }

  test("NGramsCounts") {
    sc = new SparkContext("local[2]", "NGramSuite")
    val rdd = sc.parallelize(Seq("Pipelines are awesome", "NLP is awesome"), 2)

    def run(orders: Seq[Int]) = {
      val pipeline = tokenizer then
        new NGramsFeaturizer(orders) then
        NGramsCounts

      pipeline(rdd).collect().toSet
    }

    def liftToNGram(tuples: Set[(Seq[String], Int)]) =
      tuples.map { case (toks, count) => (new NGram(toks.toArray), count) }

    val unigramCounts = Set((Seq("awesome"), 2),
      (Seq("Pipelines"), 1), (Seq("are"), 1), (Seq("NLP"), 1), (Seq("is"), 1))

    assert(run(Seq(1)) === liftToNGram(unigramCounts),
      "unigrams incorrectly counted")
    assert(run(2 to 3).forall(_._2 == 1),
      "some 2-gram or 3-gram occurs once but is incorrectly counted")
  }

}
