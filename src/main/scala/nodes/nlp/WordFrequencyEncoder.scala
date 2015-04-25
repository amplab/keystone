package nodes.nlp

import org.apache.spark.broadcast.Broadcast
import pipelines.{Estimator, Transformer}

import org.apache.spark.rdd.RDD

object WordFrequencyEncoder extends Estimator[RDD[Seq[String]], RDD[Seq[Int]]] {

  private[this] val makeUnigrams = new NGramsFeaturizer[String](1 to 1) then
    new NGramsCounts()

  // TODO: alternative approach: collectAsMap once, let driver do the work.
  def fit(data: RDD[Seq[String]]): StringToIntTransformer = {
    val unigramCounts = makeUnigrams(data)

    val wordIndex = unigramCounts
      .zipWithIndex() // indexes respect the sorted order
      .map { case ((unigram, count), index) =>
        // valid if # of word types in training data is less than Int.MaxValue
        (unigram.words(0), index.asInstanceOf[Int])
      }.collectAsMap()

    val wordIndexBroadcast = unigramCounts.sparkContext.broadcast(wordIndex)

    val unigrams = unigramCounts.map { case (unigram, count) =>
      (wordIndexBroadcast.value(unigram.words(0)), count)
    }.collectAsMap()

    new StringToIntTransformer(wordIndexBroadcast, unigrams)
  }

}

// TODO: map to -1 for out-of-vocab words?
class StringToIntTransformer(
    wordIndexBroadcast: Broadcast[scala.collection.Map[String, Int]],
    val unigramCounts: scala.collection.Map[Int, Int])
  extends Transformer[Seq[String], Seq[Int]] {

  lazy val numTokens = unigramCounts.values.sum

  def apply(in: RDD[Seq[String]]): RDD[Seq[Int]] = {
    in.map(ngram => ngram.map(wordIndexBroadcast.value(_)))
  }

}
