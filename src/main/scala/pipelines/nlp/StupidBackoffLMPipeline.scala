package pipelines.nlp

import nodes.nlp.{NGramsCounts, NGramsFeaturizer, SimpleTokenizer}
import org.apache.spark.{SparkContext, SparkConf}

class StupidBackoffLMPipeline {


  def main(args: Array[String]) {
    if (args.length < 2) {
      sys.error("usage: <sparkMaster> <trainData>")
    }
    val sparkMaster = args(0)
    val trainData = args(1)

    val conf = new SparkConf().setMaster(sparkMaster).setAppName("LanguageModelPipeline")
    val sc = new SparkContext(conf)

    val text = SimpleTokenizer(sc.textFile(trainData))

    /** Vocab generation step */
    val extractUnigrams = new NGramsFeaturizer(1 to 1) then
      new NGramsCounts
    val unigramCounts = extractUnigrams(text)

    unigramCounts // RDD[(NGram[String], Int)]


    /** NGram (n >= 2) generation step */
    // Need RDD[(NGram[Long], Int)]
    val extractNGram = new NGramsFeaturizer(2 to 5) then
      new NGramsCounts

    val intermediateNGramCounts = extractUnigrams(text)

    intermediateNGramCounts
  }

}
