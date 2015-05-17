package pipelines.nlp

import nodes.nlp._

import org.apache.spark.{SparkContext, SparkConf}

import scopt.OptionParser

object StupidBackoffPipeline {

  val appName = "StupidBackoffPipeline"

  case class StupidBackoffConfig(trainData: String = "", numParts: Int = 16, n: Int = 3)

  def parse(args: Array[String]): StupidBackoffConfig =
    new OptionParser[StupidBackoffConfig](appName) {
      head(appName, "0.1")
      opt[String]("trainData") required() action { (x, c) => c.copy(trainData = x) }
      opt[String]("numParts") required() action { (x, c) => c.copy(numParts = x.toInt) }
      opt[String]("n") optional() action { (x, c) => c.copy(n = x.toInt) }
    }.parse(args, StupidBackoffConfig()).get

  def main(args: Array[String]) {
    val appConfig = parse(args)
    val conf = new SparkConf().setAppName(appName)
    conf.setIfMissing("spark.master", "local[4]")
    val sc = new SparkContext(conf)

    val text = Tokenizer()(sc.textFile(appConfig.trainData, appConfig.numParts))

    /** Vocab generation step */
    val frequencyEncode = WordFrequencyEncoder.fit(text)
    val unigramCounts = frequencyEncode.unigramCounts

    /** NGram (n >= 2) generation step */
    val makeNGrams = frequencyEncode then
      NGramsFeaturizer[Int](2 to appConfig.n) then
      NGramsCounts[Int](NGramsCountsMode.NoAdd)

    val ngramCounts = makeNGrams(text)

    /** Stupid backoff scoring step */
    val stupidBackoff = StupidBackoffEstimator[Int](unigramCounts)
    val languageModel = stupidBackoff.fit(ngramCounts)

    /** Done: save or serve */
    languageModel.scoresRDD.cache()
    println(
      s"""|number of tokens: ${languageModel.numTokens}
          |size of vocabulary: ${languageModel.unigramCounts.size}
          |number of ngrams: ${languageModel.scoresRDD.count()}
          |""".stripMargin)
    println("trained scores of 100 ngrams in the corpus:")
    languageModel.scoresRDD.take(100).foreach(println)

    sc.stop()
  }

}
