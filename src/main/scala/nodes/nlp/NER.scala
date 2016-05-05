package nodes.nlp

import epic.sequences.{Segmentation, SemiCRF}
import org.apache.spark.rdd.RDD
import workflow.Transformer

/**
  * Transformer that uses Epic's named entity recognition on a sequence of words.
  * It is recommended to use a Tokenizer before applying the NER,
  * assuming we get a list of strings as input.
  *
  * Here's an example:
  * {{{
  *   val model = epic.models.NerSelector.loadNer("en").get
  *   val NERmodel = NERModel(model).apply(data)
  * }}}
  *
  * @param model The NER model loaded from the Epic library
  */
case class NER(@transient model: SemiCRF[Any, String])
  extends Transformer[Array[String], Segmentation[Any, String]] {

  def apply(in: Array[String]): Segmentation[Any, String] = {
    model.bestSequence(in.toIndexedSeq)
  }

  override def apply(in: RDD[Array[String]]): RDD[Segmentation[Any, String]] = {
    val modelBroadcast = in.sparkContext.broadcast(model)
    in.map(s => modelBroadcast.value.bestSequence(s.toIndexedSeq))
  }

}
