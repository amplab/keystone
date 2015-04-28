package pipelines.nlp

import nodes.nlp._
import org.apache.spark.{SparkContext, SparkConf}

object StupidBackoffLMPipeline {

  def main(args: Array[String]) {
    if (args.length < 2) {
      sys.error("usage: <sparkMaster> <trainData> <numParts>")
    }
    val sparkMaster = args(0)
    val trainData = args(1)
    val numParts = args(2).toInt

    val conf = new SparkConf().setMaster(sparkMaster).setAppName("StupidBackoffLMPipeline")
    val sc = new SparkContext(conf)

    val text = SimpleTokenizer(sc.textFile(trainData, numParts))

    /** Vocab generation step */
    val frequencyEncode = WordFrequencyEncoder.fit(text)
    val unigramCounts = frequencyEncode.unigramCounts

    /** NGram (n >= 2) generation step */
    val makeNGrams = frequencyEncode then
      new NGramsFeaturizer(2 to 5) then
      new NGramsCounts("noAdd")

    val ngramCounts = makeNGrams(text)

    /** Stupid backoff scoring step */
    val stupidBackoff = new StupidBackoffLM[Int](unigramCounts)
    val languageModel = stupidBackoff(ngramCounts)

    /** Done: save or serve */
    languageModel
      .collect()
      .foreach(println)

    sc.stop()
  }

}
