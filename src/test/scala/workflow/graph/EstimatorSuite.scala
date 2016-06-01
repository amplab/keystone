package workflow.graph

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite
import pipelines.{LocalSparkContext, Logging}

class EstimatorSuite extends FunSuite with LocalSparkContext with Logging {
  test("Estimator fit RDD") {
    sc = new SparkContext("local", "test")

    val intEstimator = new Estimator[Int, Int] {
      def fit(data: RDD[Int]): Transformer[Int, Int] = {
        val first = data.first()
        Transformer(x => x + first)
      }
    }

    val trainData = sc.parallelize(Seq(32, 94, 12))
    val testData = sc.parallelize(Seq(42, 58, 61))

    val pipeline = intEstimator.withData(trainData)
    assert(pipeline.apply(testData).get().collect().toSeq === Seq(42 + 32, 58 + 32, 61 + 32))
  }

  test("Estimator fit Pipeline Data") {
    sc = new SparkContext("local", "test")

    val transformer = Transformer[Int, Int](_ * 2)

    val intEstimator = new Estimator[Int, Int] {
      def fit(data: RDD[Int]): Transformer[Int, Int] = {
        val first = data.first()
        Transformer(x => x + first)
      }
    }

    val trainData = sc.parallelize(Seq(32, 94, 12))
    val testData = sc.parallelize(Seq(42, 58, 61))

    val pipeline = intEstimator.withData(transformer(trainData))
    assert(pipeline.apply(testData).get().collect().toSeq === Seq(42 + 64, 58 + 64, 61 + 64))
  }

}
