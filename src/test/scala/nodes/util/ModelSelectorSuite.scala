package nodes.util

import evaluation.MulticlassClassifierEvaluator
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite
import pipelines.{Logging, LocalSparkContext}
import workflow.{Pipeline, Transformer}

class ModelSelectorSuite extends FunSuite with LocalSparkContext with Logging {
  val first = Transformer[Int, Int](_ => 0)
  val second = Transformer[Int, Int](_ => 1)
  val third = Transformer[Int, Int](_ => 2)
  val fourth = Transformer[Int, Int](_ => 3)

  val totalChoices = Pipeline.gather(Seq(first, second, third, fourth))

  val evaluator: (RDD[Int], RDD[Int]) => Double = MulticlassClassifierEvaluator(_, _, 4).totalAccuracy

  test("Select First") {
    sc = new SparkContext("local", "test")

    val data = sc.parallelize(Seq(-1, -1, -1, -1))
    val labels = data.map(_ => 0)

    val pipeline = totalChoices andThen (ModelSelector(4, evaluator), data, labels)

    assert(pipeline(-1) === 0)
  }

  test("Select Second") {
    sc = new SparkContext("local", "test")

    val data = sc.parallelize(Seq(-1, -1, -1, -1))
    val labels = data.map(_ => 1)

    val pipeline = totalChoices andThen (ModelSelector(4, evaluator), data, labels)

    assert(pipeline(-1) === 1)
  }

  test("Select Third") {
    sc = new SparkContext("local", "test")

    val data = sc.parallelize(Seq(-1, -1, -1, -1))
    val labels = data.map(_ => 2)

    val pipeline = totalChoices andThen (ModelSelector(4, evaluator), data, labels)

    assert(pipeline(-1) === 2)
  }

  test("Select Fourth") {
    sc = new SparkContext("local", "test")

    val data = sc.parallelize(Seq(-1, -1, -1, -1))
    val labels = data.map(_ => 3)

    val pipeline = totalChoices andThen (ModelSelector(4, evaluator), data, labels)

    assert(pipeline(-1) === 3)
  }
}
