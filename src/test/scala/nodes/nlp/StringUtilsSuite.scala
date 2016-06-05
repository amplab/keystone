package nodes.nlp

import org.apache.spark.SparkContext
import org.scalatest.FunSuite
import pipelines.PipelineContext

class StringUtilsSuite extends FunSuite with PipelineContext {
  val stringToManip = Array("  The quick BROWN fo.X ", " ! !.,)JumpeD. ovER the LAZy DOG.. ! ")
  test("trim") {
    sc = new SparkContext("local", "test")
    val out = Trim.apply(sc.parallelize(stringToManip, 1)).collect().toSeq
    assert(out === Seq("The quick BROWN fo.X", "! !.,)JumpeD. ovER the LAZy DOG.. !"))
  }

  test("lower case") {
    sc = new SparkContext("local", "test")
    val out = LowerCase().apply(sc.parallelize(stringToManip, 1)).collect().toSeq
    assert(out === Seq("  the quick brown fo.x ", " ! !.,)jumped. over the lazy dog.. ! "))
  }

  test("tokenizer") {
    sc = new SparkContext("local", "test")
    val out = Tokenizer().apply(sc.parallelize(stringToManip, 1)).collect().toSeq
    assert(out === Seq(Seq("", "The", "quick", "BROWN", "fo", "X"), Seq("", "JumpeD", "ovER", "the", "LAZy", "DOG")))
  }
}
