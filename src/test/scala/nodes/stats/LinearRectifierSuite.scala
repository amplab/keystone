package nodes.stats

import breeze.linalg.DenseMatrix
import breeze.stats.distributions.Rand
import org.apache.spark.SparkContext
import org.scalatest.FunSuite
import pipelines._
import utils.MatrixUtils

class LinearRectifierSuite extends FunSuite with LocalSparkContext with Logging {

  def createRandomMatrix(numRows: Int, numCols: Int, numParts: Int) = {
    val rowsPerPart = numRows / numParts
    val matrixParts = sc.parallelize(1 to numParts, numParts).mapPartitions { part =>
      Iterator(DenseMatrix.rand(rowsPerPart, numCols, Rand.gaussian))
    }
    matrixParts.cache()
  }

  test("Test MaxVal") {
    sc = new SparkContext("local", "test")
    val matrixParts = createRandomMatrix(128, 16, 4)

    val x = matrixParts.flatMap(MatrixUtils.matrixToRowArray)
    val y = x.map(r => r.forall(_ >= 0.0))

    val valmaxNode = LinearRectifier()
    val maxy = valmaxNode.apply(x).map(r => r.forall(_ >= 0.0))

    //The random matrix should *not* all be >= 0
    assert(!y.reduce {(a,b) => a | b})

    //The valmax'ed random matrix *should* all be >= 0.
    assert(maxy.reduce {(a,b) => a | b})
  }
}
