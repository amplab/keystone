package pipelines.nlp

import nodes.nlp._

import org.apache.spark.{SparkContext, SparkConf}

object StupidBackoffPipeline {

  def main(args: Array[String]) {
    if (args.length < 2) {
      sys.error("usage: <sparkMaster> <trainData> <numParts>")
    }
    val sparkMaster = args(0)
    val trainData = args(1)
    val numParts = args(2).toInt

    val conf = new SparkConf().setMaster(sparkMaster).setAppName("StupidBackoffPipeline")
    val sc = new SparkContext(conf)

    val text = SimpleTokenizer(sc.textFile(trainData, numParts))

    /** Vocab generation step */
    val frequencyEncode = WordFrequencyEncoder.fit(text)
    val unigramCounts = frequencyEncode.unigramCounts

    /** NGram (n >= 2) generation step */
    val makeNGrams = frequencyEncode then
      NGramsFeaturizer(2 to 5) then
      NGramsCounts("noAdd")

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
