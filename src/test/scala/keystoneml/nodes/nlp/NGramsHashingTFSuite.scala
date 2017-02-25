package keystoneml.nodes.nlp

import org.scalatest.FunSuite
import keystoneml.workflow.PipelineContext

class NGramsHashingTFSuite extends FunSuite with PipelineContext {

  test("NGramsHashingTF 1 to 1") {
    val dims = 40000

    val testDatum = "this sentence is a sentence is the some there some then there some".split(" ")
    val ngrams = NGramsFeaturizer(1 to 1).apply(testDatum)
    val tfVector = HashingTF(dims).apply(ngrams)

    val ngramsHashingTFVector = NGramsHashingTF(1 to 1, dims).apply(testDatum)

    // Assert that the NGramsHashingTF node returns the same output as first getting n-grams then hashing
    assert(ngramsHashingTFVector === tfVector)
  }

  test("NGramsHashingTF 1 to 3") {
    val dims = 40000

    val testDatum = "this sentence is a sentence is the some there some then there some".split(" ")
    val ngrams = NGramsFeaturizer(1 to 3).apply(testDatum)
    val tfVector = HashingTF(dims).apply(ngrams)

    val ngramsHashingTFVector = NGramsHashingTF(1 to 3, dims).apply(testDatum)

    // Assert that the NGramsHashingTF node returns the same output as first getting n-grams then hashing
    assert(ngramsHashingTFVector === tfVector)
  }

  test("NGramsHashingTF 2 to 3") {
    val dims = 40000

    val testDatum = "this sentence is a sentence is the some there some then there some".split(" ")
    val ngrams = NGramsFeaturizer(2 to 3).apply(testDatum)
    val tfVector = HashingTF(dims).apply(ngrams)

    val ngramsHashingTFVector = NGramsHashingTF(2 to 3, dims).apply(testDatum)

    // Assert that the NGramsHashingTF node returns the same output as first getting n-grams then hashing
    assert(ngramsHashingTFVector === tfVector)
  }

  test("NGramsHashingTF with collisions 1 to 3") {
    val dims = 6

    val testDatum = "this sentence is a sentence is the some there some then there some".split(" ")
    val ngrams = NGramsFeaturizer(1 to 3).apply(testDatum)
    val tfVector = HashingTF(dims).apply(ngrams)

    val ngramsHashingTFVector = NGramsHashingTF(1 to 3, dims).apply(testDatum)

    // Assert that the NGramsHashingTF node returns the same output as first getting n-grams then hashing
    assert(ngramsHashingTFVector === tfVector)
  }
}
