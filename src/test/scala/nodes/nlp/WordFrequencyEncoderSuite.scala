package nodes.nlp

import pipelines.LocalSparkContext

import org.apache.spark.SparkContext

import org.scalatest.FunSuite

class WordFrequencyEncoderSuite extends FunSuite with LocalSparkContext {

  val text = Seq("Winter coming", "Winter Winter is coming")

  test("WordFrequencyEncoder") {
    sc = new SparkContext("local[2]", "WordFrequencyEncoderSuite")
    val rdd = SimpleTokenizer(sc.parallelize(text, 2))
    val encoder = WordFrequencyEncoder.fit(rdd)

    assert(encoder(rdd).collect().sameElements(Seq(Seq(0, 1), Seq(0, 0, 2, 1))),
      "frequency-encoded result incorrect")
    assert(encoder.numTokens === 6,
      "fitted value numTokens incorrect")
    assert(encoder.unigramCounts === Map(0 -> 3, 1 -> 2, 2 -> 1),
      "fitted value unigramCounts incorrect")

    assert(encoder(sc.parallelize(Seq(Seq("hi")), 1)).collect() === Array(Seq(-1)),
      "OOV words not mapped to -1")
  }

}
