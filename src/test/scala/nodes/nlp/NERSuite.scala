package nodes.nlp

import epic.sequences.{Segmentation, SemiCRF}
import org.apache.spark.SparkContext
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar
import pipelines.{PipelineContext, Logging}

class NERSuite extends FunSuite
  with PipelineContext with Logging with MockitoSugar {

  val SENTENCE = "Hello my name is Nikolsky and I live in Quebec City".split(" ")
  val EMPTY_ANNOTATED_SENTENCE = Segmentation(IndexedSeq(), IndexedSeq())
  val model: SemiCRF[Any, String] = epic.models.NerSelector.loadNer("en").get

  test("Apply method should call SemiCRF properly") {
    val nerModelMock = mock[SemiCRF[Any, String]]
    when(nerModelMock.bestSequence(SENTENCE)).thenReturn(EMPTY_ANNOTATED_SENTENCE)

    NER(nerModelMock).apply(SENTENCE)

    verify(nerModelMock, atLeastOnce()).bestSequence(SENTENCE)
  }

  test("Apply method on Spark should call SemiCRF properly") {
    sc = new SparkContext("local", "test")
    val nerModelMock = mock[SemiCRF[Any, String]](withSettings().serializable())
    when(nerModelMock.bestSequence(SENTENCE)).thenReturn(EMPTY_ANNOTATED_SENTENCE)

    NER(nerModelMock).apply(sc.parallelize(Seq(SENTENCE)))

    verify(nerModelMock, atLeastOnce()).bestSequence(SENTENCE)
  }

  test("A named entity recognized sequence should be properly segmented") {
    val nerSequence = NER(model).apply(SENTENCE)
    verifySegmentation(nerSequence)
  }

  test("A named entity recognized sequence on Spark should be properly segmented") {
    sc = new SparkContext("local", "test")
    val nerSequence = NER(model).apply(sc.parallelize(Seq(SENTENCE)).first())
    verifySegmentation(nerSequence)
  }

  def verifySegmentation(segmentation: Segmentation[Any, String]) = {
    assert(segmentation.asFlatTaggedSequence.pairs(0)._1.isEmpty)
    assert(segmentation.asFlatTaggedSequence.pairs(0)._2 == "Hello")
    assert(segmentation.asFlatTaggedSequence.pairs(4)._1.get == "PER")
    assert(segmentation.asFlatTaggedSequence.pairs(4)._2 == "Nikolsky")
    assert(segmentation.asFlatTaggedSequence.pairs(9)._1.get == "LOC")
    assert(segmentation.asFlatTaggedSequence.pairs(9)._2 == "Quebec")
  }

}
