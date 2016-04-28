package nodes.nlp

import epic.sequences.{CRF, Segmentation, TaggedSequence}
import epic.trees.AnnotatedLabel
import workflow.Transformer
import breeze.linalg.Counter2
import org.apache.spark.rdd.RDD

case class POSTagger(@transient tagger: CRF[AnnotatedLabel, String])
  extends Transformer[Array[String], Array[(String, String)]] {
  override def apply(in: Array[String]): Array[(String, String)] = {
    val annotatedLabels: TaggedSequence[AnnotatedLabel, String] = tagger.bestSequence(in.toIndexedSeq)
    annotatedLabels.pairs.map(pair => (pair._1.label, pair._2)).toArray
  }
}

