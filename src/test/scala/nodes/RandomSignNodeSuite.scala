package nodes

import pipelines._

import org.scalatest.{FlatSpec, FunSuite}
import pipelines.{Logging, LocalSparkContext}
import org.apache.spark.SparkContext
import org.scalatest.matchers.ShouldMatchers
import breeze.linalg._
import breeze.numerics._
import breeze.stats._

class RandomSignNodeSuite extends FlatSpec with LocalSparkContext with Logging with ShouldMatchers {
  "RandomSignNode" should "flip signs according to a given vector" in {
    sc = new SparkContext("local", "test")
    val signs = DenseVector(1.0, -1.0, 1.0)
    val node = RandomSignNode(signs)
    val data: DenseVector[Double] = DenseVector(1.0, 2.0, 3.0)
    val in = sc.parallelize(Seq(data))
    val result = node.apply(in).collect
    Seq(result(0)) should equal (Seq(DenseVector(1.0, -2.0, 3.0)))
  }

  "RandomSignNode.create" should "create sane random sign vectors." in {
    val node = RandomSignNode.create(1000)
    
    node.signs.foreach(elt => assert(elt == -1.0 || elt == 1.0))
  }
}
