package internals

import nodes.util.Identity
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite
import pipelines.{LocalSparkContext, Logging}

class OperatorSuite extends FunSuite with LocalSparkContext with Logging {
  test("Datum Operator") {
    sc = new SparkContext("local", "test")

  }

  // TODO
  test("Dataset Operator") {
    sc = new SparkContext("local", "test")

  }

  // TODO: For transformer operator:
  // test delegation & laziness of single transform
  // test delegation & laziness of bulk transform
  // ensure error if not ((all datum) or (all dataset))

  // TODO for estimator operator:
  // Test laziness

  // TODO for delegating operator:
  // test delegation & laziness of single transform
  // test delegation & laziness of bulk transform
  // ensure error if not ((all datum) or (all dataset))

}
