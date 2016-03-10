package nodes.stats

import breeze.linalg.DenseMatrix
import breeze.stats.distributions.Rand
import org.apache.spark.SparkContext
import org.scalatest.FunSuite
import pipelines._
import utils.{TestUtils, MatrixUtils}

class LinearRectifierSuite extends FunSuite with LocalSparkContext with Logging {

  test("Test MaxVal") {
    sc = new SparkContext("local", "test")
    val matrixParts = TestUtils.createRandomMatrix(sc, 128, 16, 4).rdd.map(_.mat)

    val x = matrixParts.flatMap(y => MatrixUtils.matrixToRowArray(y))
    val y = x.map(r => r.forall(_ >= 0.0))

    val valmaxNode = LinearRectifier()
    val maxy = valmaxNode.apply(x).map(r => r.forall(_ >= 0.0))

    //The random matrix should *not* all be >= 0
    assert(!y.reduce {(a,b) => a | b})

    //The valmax'ed random matrix *should* all be >= 0.
    assert(maxy.reduce {(a,b) => a | b})
  }
}
