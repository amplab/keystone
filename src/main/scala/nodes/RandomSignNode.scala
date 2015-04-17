package nodes

import pipelines._
import java.util.Random
import pipelines.Pipelines.PipelineNode
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

case class RandomSignNode(val signs: Array[DataType])
    extends PipelineNode[RDD[Array[DataType]], RDD[Array[DataType]]] with Serializable {
  def apply(in: RDD[Array[DataType]]): RDD[Array[DataType]] = {
    val signsb = in.context.broadcast(signs)
    in.map(row => RandomSignNode.hadamardProduct(row, signsb.value))
  }
}

object RandomSignNode {
  def create(size: Int, random: Random): RandomSignNode = {
    val signs = new Array[DataType](size)
    for (i <- 0 until size) {
      signs(i) = if (random.nextBoolean()) 1.0 else -1.0
    }
    RandomSignNode(signs)
  }

  def hadamardProduct(a: Array[DataType], b: Array[DataType]): Array[DataType] = {
    assert(a.size == b.size, "RandomSignNode: Input dimension %d does not match output dimension %d".format(a.size, b.size))
    val size = a.size
    val result = new Array[DataType](size)
    var i = 0
    while (i < size) {
      result(i) = a(i)*b(i)
      i += 1
    }
    result
  }
}
