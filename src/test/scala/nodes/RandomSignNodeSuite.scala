package nodes

import pipelines._

import breeze.linalg._
import breeze.numerics._
import breeze.stats._

import org.apache.spark.SparkContext
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.{FlatSpec, FunSuite}

import pipelines.{Logging, LocalSparkContext}

class RandomSignNodeSuite extends FunSuite with LocalSparkContext with Logging with ShouldMatchers {

  test("RandomSignNode") {
    sc = new SparkContext("local", "test")
    val signs = DenseVector(1.0, -1.0, 1.0)
    val node = RandomSignNode(signs)
    val data: DenseVector[Double] = DenseVector(1.0, 2.0, 3.0)
    val in = sc.parallelize(Seq(data))
    val result = node.apply(in).collect
    Seq(result(0)) should equal (Seq(DenseVector(1.0, -2.0, 3.0)))
  }

  test("RandomSignNode.create") {
    val node = RandomSignNode.create(1000)
    
    node.signs.foreach(elt => assert(elt == -1.0 || elt == 1.0))
  }
}
