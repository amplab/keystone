/**
 * Created by tomerk11 on 6/12/15.
 */
package object workflow {
  implicit class PipelineDSL[A, B](val pipeline: Pipeline[A, B]) extends AnyVal {

  }

  implicit def transformerToPipeline[A, B](transformer: Transformer[A, B]): Pipeline[A, B] = {
    Pipeline[A, B](Seq(transformer), Seq(Seq(Pipeline.SOURCE)), Seq(Seq()), 0)
  }

  implicit def transformerToPipelineDSL[A, B](transformer: Transformer[A, B]): PipelineDSL[A, B] = {
    Pipeline[A, B](Seq(transformer), Seq(Seq(Pipeline.SOURCE)), Seq(Seq()), 0)
  }

}
