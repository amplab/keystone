package nodes.misc

import nodes.nlp.TermFrequency
import org.apache.spark.SparkContext
import org.scalatest.FunSuite
import pipelines.LocalSparkContext

class TermFrequencySuite extends FunSuite with LocalSparkContext {
  test("term frequency of simple strings") {
    sc = new SparkContext("local", "test")
    val in = Seq(Seq[Any]("b", "a", "c", "b", "b", "a", "b"))
    val out = TermFrequency().apply(sc.parallelize(in)).first().toMap
    assert(out === Map("a" -> 2, "b" -> 4, "c" -> 1))
  }

  test("term frequency of varying types") {
    sc = new SparkContext("local", "test")
    val in = Seq(Seq("b", "a", "c", ("b", "b"), ("b", "b"), 12, 12, "a", "b", 12))
    val out = TermFrequency().apply(sc.parallelize(in)).first().toMap
    assert(out === Map("a" -> 2, "b" -> 2, "c" -> 1, ("b", "b") -> 2, 12 -> 3))
  }

  test("log term frequency") {
    sc = new SparkContext("local", "test")
    val in = Seq(Seq[Any]("b", "a", "c", "b", "b", "a", "b"))
    val out = TermFrequency(x => math.log(x + 1)).apply(sc.parallelize(in)).first().toMap
    assert(out === Map("a" -> math.log(3), "b" -> math.log(5), "c" -> math.log(2)))
  }
}
