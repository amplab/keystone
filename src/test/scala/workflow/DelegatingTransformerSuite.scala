package workflow

import org.apache.spark.SparkContext
import org.scalatest.FunSuite
import pipelines.{Logging, LocalSparkContext}

class DelegatingTransformerSuite extends FunSuite with LocalSparkContext with Logging {
  test("single apply") {
    val hashTransformer = Transformer[String, Int](_.hashCode)
    val delegatingTransformer = new DelegatingTransformer[Int]("label")

    val string = "A31DFSsafds*be31"
    assert(delegatingTransformer.transform(Seq(string), Seq(hashTransformer)) === string.hashCode)
  }

  test("rdd apply") {
    sc = new SparkContext("local", "test")

    val hashTransformer = Transformer[String, Int](_.hashCode)
    val delegatingTransformer = new DelegatingTransformer[Int]("label")

    val strings = Seq("A31DFSsafds*be31", "lj32fsd", "woadsf8923")
    val transformedStrings = delegatingTransformer.transformRDD(
      Seq(sc.parallelize(strings)),
      Seq(hashTransformer)).collect()
    assert(transformedStrings.toSeq === strings.map(_.hashCode))
  }

}
