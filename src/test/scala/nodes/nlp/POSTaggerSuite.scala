package nodes.nlp

import epic.sequences.{CRF, TaggedSequence}
import epic.trees.AnnotatedLabel
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import pipelines.Logging

class POSTaggerSuite extends FunSuite with Logging with MockitoSugar {

  val SENTENCE = "Hello my name is Nikolsky and I like bananas".split(" ")
  val EMPTY_ANNOTATED_SENTENCE = TaggedSequence(IndexedSeq(), IndexedSeq())

  test("apply calls CRF tagger properly") {
    val taggerMock = mock[CRF[AnnotatedLabel, String]]
    when(taggerMock.bestSequence(SENTENCE)).thenReturn(EMPTY_ANNOTATED_SENTENCE)

    POSTagger(taggerMock).apply(SENTENCE)

    verify(taggerMock, atLeastOnce()).bestSequence(SENTENCE)
  }

  /**
    * This test is just to make sure the POS Tagger is acting properly
    */
  test("CRF tagger return tagged words properly") {
    val tagger: CRF[AnnotatedLabel, String] = epic.models.PosTagSelector.loadTagger("en").get

    val taggedSequence = POSTagger(tagger).apply(SENTENCE)

    assert(taggedSequence.length == 9, "Should have 9 labeled tokens")
    assert(taggedSequence(2)._1 == "NN", "name should be a noun")
    assert(taggedSequence(3)._1 == "VBZ", "is should be a verb")
    assert(taggedSequence(4)._1 == "NNP", "Nikoslky should be a proper noun")
    assert(taggedSequence(8)._1 == "NNS", "bananas should be a noun, plural")
  }

}
