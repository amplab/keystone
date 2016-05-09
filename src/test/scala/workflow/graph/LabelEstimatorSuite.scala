package workflow.graph

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite
import pipelines.{LocalSparkContext, Logging}

class LabelEstimatorSuite extends FunSuite with LocalSparkContext with Logging {
  test("estimator withData") {
    sc = new SparkContext("local", "test")

    val intEstimator = new LabelEstimator[Int, Int, String] {
      protected def fitRDDs(data: RDD[Int], labels: RDD[String]): Transformer[Int, Int] = {
        val first = data.first()
        val label = labels.first().hashCode
        Transformer(_ => first + label)

      }
    }

    val trainData = sc.parallelize(Seq(32, 94, 12))
    val trainLabels = sc.parallelize(Seq("sjkfdl", "iw", "432"))
    val testData = sc.parallelize(Seq(42, 58, 61))

    val pipeline = intEstimator.fit(trainData, trainLabels)
    assert(pipeline.apply(testData).get().collect().toSeq === Seq.fill(3)(32 + "sjkfdl".hashCode))
  }
}
