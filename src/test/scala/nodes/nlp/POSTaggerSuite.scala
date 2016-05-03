package nodes.nlp

import epic.sequences.{CRF, TaggedSequence}
import epic.trees.AnnotatedLabel
import org.apache.spark.SparkContext
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import pipelines.{LocalSparkContext, Logging}

class POSTaggerSuite extends FunSuite
   with LocalSparkContext with Logging with MockitoSugar {

  val SENTENCE = "Hello my name is Nikolsky and I like bananas".split(" ")
  val EMPTY_ANNOTATED_SENTENCE = TaggedSequence(IndexedSeq(), IndexedSeq())
  val model = epic.models.PosTagSelector.loadTagger("en").get

  test("Apply method should call CRF properly") {
    val taggerMock = mock[CRF[AnnotatedLabel, String]]
    when(taggerMock.bestSequence(SENTENCE)).thenReturn(EMPTY_ANNOTATED_SENTENCE)

    POSTagger(taggerMock).apply(SENTENCE)

    verify(taggerMock, atLeastOnce()).bestSequence(SENTENCE)
  }

  test("Apply method on Spark should call CRF properly") {
    sc = new SparkContext("local", "test")
    val taggerMock = mock[CRF[AnnotatedLabel, String]](withSettings().serializable())
    when(taggerMock.bestSequence(SENTENCE)).thenReturn(EMPTY_ANNOTATED_SENTENCE)

    POSTagger(taggerMock).apply(sc.parallelize(Seq(SENTENCE)))

    verify(taggerMock, atLeastOnce()).bestSequence(SENTENCE)
  }

  test("A tagged sequence should be properly tagged") {
    val taggedSequence = POSTagger(model).apply(SENTENCE)
    verifyTaggedSequence(taggedSequence)
  }

  test("A tagged sequence on Spark should be properly tagged") {
    sc = new SparkContext("local", "test")
    val taggedSequence = POSTagger(model).apply(sc.parallelize(Seq(SENTENCE)).first())
    verifyTaggedSequence(taggedSequence)
  }

  def verifyTaggedSequence(taggedSequence: TaggedSequence[AnnotatedLabel, String]) = {
    assert(taggedSequence.pairs(2)._1.label == "NN")
    assert(taggedSequence.pairs(2)._2 == "name")
    assert(taggedSequence.pairs(3)._1.label == "VBZ")
    assert(taggedSequence.pairs(3)._2 == "is")
    assert(taggedSequence.pairs(4)._1.label == "NNP")
    assert(taggedSequence.pairs(4)._2 == "Nikolsky")
    assert(taggedSequence.pairs(8)._1.label == "NNS")
    assert(taggedSequence.pairs(8)._2 == "bananas")
  }

}
