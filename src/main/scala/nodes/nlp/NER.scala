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
  *   val NER = NER(model).apply(data)
  * }}}
  *
  * @param model The NER model loaded from the Epic library
  */
case class NER[T](@transient model: SemiCRF[T, String])
  extends Transformer[Array[String], Segmentation[T, String]] {

  def apply(in: Array[String]): Segmentation[T, String] = {
    model.bestSequence(in.toIndexedSeq)
  }

  override def apply(in: RDD[Array[String]]): RDD[Segmentation[T, String]] = {
    val modelBroadcast = in.sparkContext.broadcast(model)
    in.map(s => modelBroadcast.value.bestSequence(s.toIndexedSeq))
  }

}
