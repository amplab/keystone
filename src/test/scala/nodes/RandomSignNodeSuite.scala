package nodes

import pipelines._

import org.scalatest.{FlatSpec, FunSuite}
import pipelines.{Logging, LocalSparkContext}
import org.apache.spark.SparkContext
import org.scalatest.matchers.ShouldMatchers

class RandomSignNodeSuite extends FlatSpec with LocalSparkContext with Logging with ShouldMatchers {
  "RandomSignNode" should "flip signs according to a given vector" in {
    sc = new SparkContext("local", "test")
    val signs = Seq(1.0, -1.0, 1.0).toArray
    val node = RandomSignNode(signs)
    val data: Array[DataType] = Seq(1.0, 2.0, 3.0).toArray
    val in = sc.parallelize(Seq(data))
    val result = node.apply(in).collect
    result(0).toSeq should equal (Seq(1.0, -2.0, 3.0))
  }

  "RandomSignNode.create" should "create sane random sign vectors." in {
    val random = new java.util.Random(0L)
    val node = RandomSignNode.create(1000, random)
    node.signs.foreach(elt => assert(elt == -1.0 || elt == 1.0))
    val theSum = node.signs.sum
    assert(theSum > -200.0 && theSum < 200)
  }
}
