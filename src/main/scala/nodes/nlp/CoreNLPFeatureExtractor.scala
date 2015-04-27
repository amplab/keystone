package nodes.nlp

import edu.arizona.sista.processors.Processor
import edu.arizona.sista.processors.fastnlp.FastNLPProcessor
import org.apache.spark.rdd.RDD
import pipelines.Transformer

/**
 * TODO: Fill out
 * CoreNLP: tokenizes, lemmatizes, extracts entities (replacing token w/ the type of entity), takes NGRAMS
 */
case class CoreNLPFeatureExtractor(orders: Seq[Int]) extends Transformer[String, Seq[String]] {
  object CoreNLPContainer {
    @transient lazy val proc = new FastNLPProcessor()
  }

  override def apply(in: RDD[String]): RDD[Seq[String]] = {
    in.map(x => getNgrams(x, CoreNLPContainer.proc))
  }

  def getNgrams(f : String, proc : Processor): Seq[String] = {
    val doc = proc.mkDocument(f)
    proc.tagPartsOfSpeech(doc)
    proc.lemmatize(doc)
    proc.recognizeNamedEntities(doc)
    doc.clear()
    val out = doc.sentences.map(s => {
      val out = new Array[String](s.words.length)
      for (i <- 0 to s.words.length - 1) {
        out(i) = if (s.entities.get(i) != "O") s.entities.get(i) else normalize(s.lemmas.get(i))
      }
      out
    })
    orders.map(n => {
      out.map(s => {
        s.sliding(n).map(gram => gram.mkString(" ")).toList
      }).flatMap(identity).toList
    }).flatMap(identity).toList
  }

  def normalize(s : String): String = {
    val pattern = "[^a-zA-Z0-9\\s+]"
    pattern.r.replaceAllIn(s,pattern=>"").toLowerCase
  }
}
