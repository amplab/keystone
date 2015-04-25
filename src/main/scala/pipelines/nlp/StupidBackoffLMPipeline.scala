package pipelines.nlp

import nodes.nlp.{WordFrequencyEncoder, NGramsCounts, NGramsFeaturizer, SimpleTokenizer}
import org.apache.spark.{SparkContext, SparkConf}

object StupidBackoffLMPipeline {

  def main(args: Array[String]) {
    if (args.length < 2) {
      sys.error("usage: <sparkMaster> <trainData>")
    }
    val sparkMaster = args(0)
    val trainData = args(1)

    val conf = new SparkConf().setMaster(sparkMaster).setAppName("StupidBackoffLMPipeline")
    val sc = new SparkContext(conf)

    val text = SimpleTokenizer(sc.textFile(trainData))

    /** Vocab generation step */
    val frequencyEncode = WordFrequencyEncoder.fit(text)

    /** NGram (n >= 2) generation step */
    // Need RDD[(NGram[Long], Int)]
    val makeNGrams = frequencyEncode then
      new NGramsFeaturizer(2 to 5) then
      new NGramsCounts("noAdd")

    val rawNGramCounts = makeNGrams(text)

    rawNGramCounts
      .collect()
      .foreach(println)

    // TODO: init bigram partition

//    val model = StupidBackoffScores(rawNGramCounts).count()

    sc.stop()
  }

}
