package keystoneml.nodes.nlp

import org.apache.spark.SparkContext
import org.scalatest.FunSuite
import keystoneml.pipelines.Logging
import keystoneml.workflow.PipelineContext

class CoreNLPFeatureExtractorSuite extends FunSuite with PipelineContext with Logging {
  test("lemmatization") {
    sc = new SparkContext("local", "test")

    val text = "jumping snakes lakes oceans hunted"
    val tokens = CoreNLPFeatureExtractor(1 to 3).apply(sc.parallelize(Seq(text))).first().toSet

    // Make sure at least very simple cases were lemmatized
    assert(tokens.contains("jump"))
    assert(tokens.contains("snake"))
    assert(tokens.contains("lake"))
    assert(tokens.contains("ocean"))
    assert(tokens.contains("hunt"))

    // Assert the unlemmatized tokens are no longer there
    assert(!tokens.contains("jumping"))
    assert(!tokens.contains("snakes"))
    assert(!tokens.contains("oceans"))
    assert(!tokens.contains("lakes"))
    assert(!tokens.contains("hunted"))
  }

  test("entity extraction") {
    sc = new SparkContext("local", "test")

    val text = "John likes cake and he lives in Florida"
    val tokens = CoreNLPFeatureExtractor(1 to 3).apply(sc.parallelize(Seq(text))).first().toSet

    // Make sure at least very simple entities were identified and extracted
    assert(tokens.contains("PERSON"))
    assert(tokens.contains("LOCATION"))

    // Assert the original tokens are no longer there
    assert(!tokens.contains("John"))
    assert(!tokens.contains("Florida"))
  }

  test("1-2-3-grams") {
    sc = new SparkContext("local", "test")

    val text = "a b c d"
    val tokens = CoreNLPFeatureExtractor(1 to 3).apply(sc.parallelize(Seq(text))).first().toSet

    // Make sure expected unigrams appear
    assert(tokens.contains("a"))
    assert(tokens.contains("b"))
    assert(tokens.contains("c"))
    assert(tokens.contains("d"))

    // Make sure expected bigrams appear
    assert(tokens.contains("a b"))
    assert(tokens.contains("b c"))
    assert(tokens.contains("c d"))

    // Make sure expected 3-grams appear
    assert(tokens.contains("a b c"))
    assert(tokens.contains("b c d"))
  }
}
