package nodes.nlp

import epic.sequences.{CRF, TaggedSequence}
import epic.trees.AnnotatedLabel
import workflow.Transformer
import org.apache.spark.rdd.RDD

/**
  * Transformer that uses Epic to part-of-speech tag a sequence of words.
  * It is recommended to use a Tokenizer before applying the POS Tagger,
  * assuming we get a list of strings as input.
  *
  * Note: Epic model is kind of heavy (~300MB) so it may take some time to load
  * TODO: Test the model on a Spark cluster w/ and w/o broadcast variable for benchmark
  *
  * Here's an example:
  * {{{
  *   val model = epic.models.PosTagSelector.loadTagger("en").get
  *   val POSTagger = POSTagger(model).apply(data)
  * }}}
  *
  * @param model The POS Tagger model loaded from the Epic library
  */
case class POSTagger(model: CRF[AnnotatedLabel, String])
  extends Transformer[Array[String], TaggedSequence[AnnotatedLabel, String]] {

  def apply(in: Array[String]): TaggedSequence[AnnotatedLabel, String] = {
    model.bestSequence(in.toIndexedSeq)
  }

  override def apply(in: RDD[Array[String]]): RDD[TaggedSequence[AnnotatedLabel, String]] = {
    val modelBroadcast = in.sparkContext.broadcast(model)
    in.map(s => modelBroadcast.value.bestSequence(s.toIndexedSeq))
  }

}
