package nodes.nlp

import pipelines.{Estimator, Transformer}

import org.apache.spark.rdd.RDD

object WordFrequencyEncoder extends Estimator[RDD[Seq[String]], RDD[Seq[Int]]] {

  private[this] val makeUnigrams = new NGramsFeaturizer[String](1 to 1) then
    new NGramsCounts()

  def fit(data: RDD[Seq[String]]): StringToIntTransformer = {
    val unigramCounts = makeUnigrams(data)

    val wordIndex = unigramCounts
      .zipWithIndex() // indexes respect the sorted order
      .map { case ((unigram, count), index) =>
      // valid if # of word types in training data is less than Int.MaxValue
      (unigram.words(0), index.asInstanceOf[Int])
    }.collectAsMap()

    new StringToIntTransformer(wordIndex, unigramCounts)
  }

}

class StringToIntTransformer(
    wordIndex: scala.collection.Map[String, Int],
    val unigramCounts: RDD[(NGram[String], Int)])
  extends Transformer[Seq[String], Seq[Int]] {

  def apply(in: RDD[Seq[String]]): RDD[Seq[Int]] = {
    val wordIndexBroadcast = in.sparkContext.broadcast(wordIndex)
    in.map(ngram => ngram.map(wordIndexBroadcast.value(_)))
  }

}
