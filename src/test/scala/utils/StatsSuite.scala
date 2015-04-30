package utils

import breeze.linalg._

import org.scalatest.{FlatSpec, FunSuite}
import org.apache.spark.SparkContext
import org.scalatest.matchers.ShouldMatchers

import pipelines._

class StatsSuite extends FunSuite with LocalSparkContext with Logging with ShouldMatchers {
  test("signedHellingerMapper") {
    sc = new SparkContext("local", "test")
    val x = Seq(DenseVector(4.0, -16.0, 36.0, -1, -49))
    val in = sc.parallelize(x)
    val result = Stats.signedHellingerMapper(in).collect
    result.toSeq should equal (Seq(DenseVector(2.0, -4.0, 6.0, -1, -7)))   
  }
}
