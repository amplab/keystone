package nodes.learning

import breeze.linalg.{DenseVector, DenseMatrix}
import breeze.stats.distributions.Rand
import org.apache.spark.SparkContext
import org.scalatest.FunSuite
import pipelines._
import utils.Stats

class BlockLinearMapperSuite extends FunSuite with LocalSparkContext with Logging {

  test("BlockLinearMapper transformation") {
    sc = new SparkContext("local", "test")

    val inDims = 1000
    val outDims = 100
    val numChunks = 5
    val numPerChunk = inDims/numChunks

    val mat = DenseMatrix.rand(inDims, outDims, Rand.gaussian)
    val vec = DenseVector.rand(inDims, Rand.gaussian)

    val splitVec = (0 until numChunks).map(i => vec((numPerChunk*i) until (numPerChunk*i + numPerChunk)))
    val splitMat = (0 until numChunks).map(i => mat((numPerChunk*i) until (numPerChunk*i + numPerChunk), ::))

    val linearMapper = new LinearMapper(mat, DenseVector.zeros(mat.cols))
    val blockLinearMapper = new BlockLinearMapper(splitMat, DenseVector.zeros(mat.cols))

    assert(Stats.aboutEq(blockLinearMapper(splitVec), linearMapper(vec), 1e-4))
  }
}
