package nodes.nlp

import epic.sequences.{CRF, TaggedSequence}
import epic.trees.AnnotatedLabel
import workflow.Transformer
import org.apache.spark.broadcast.Broadcast

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
  override def apply(in: Array[String]): TaggedSequence[AnnotatedLabel, String] = {
    model.bestSequence(in.toIndexedSeq)
  }
}

/**
  * Here's an example using it with a broadcast variable:
  * {{{
  *   val model = epic.models.PosTagSelector.loadTagger("en").get
  *   val broadcastModel = sc.broadcast(model)
  *   val POSTagger = BroadcastPOSTagger(broadcastModel).apply(data)
  * }}}
  *
  * @param model The Broadcasted POS Tagger model loaded from the Epic library
  */
case class BroadcastPOSTagger(model: Broadcast[CRF[AnnotatedLabel, String]])
  extends Transformer[Array[String], TaggedSequence[AnnotatedLabel, String]] {
  override def apply(in: Array[String]): TaggedSequence[AnnotatedLabel, String] = {
    model.value.bestSequence(in.toIndexedSeq)
  }
}
