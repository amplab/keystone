package pipelines

import pipelines.nlp.NGrams

import org.apache.spark.SparkContext

import org.scalatest._

// TODO: properly compare
class NGramSuite extends FunSuite with LocalSparkContext {

  test("NGram transformers") {
    val sc = new SparkContext("local[2]", "NGramSuite")
    val rdd = sc.parallelize(Seq("Pipelines are awesome", "NLP is awesome"), 2)

    def run(orders: Seq[Int]) = {
      val pipeline = new NGrams(orders)
      val counts = pipeline.apply(rdd).collect()
      counts.foreach(println)
      println()
    }

    run(Seq(1))
    run(1 to 2)
    run(1 to 3)
    run(2 to 5)
  }

}
