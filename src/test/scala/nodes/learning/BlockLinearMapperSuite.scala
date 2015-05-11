package nodes.learning

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

    val mat = Stats.randMatrixGaussian(inDims, outDims)
    val vec = Stats.randMatrixGaussian(1, inDims).toDenseVector

    val splitVec = (0 until numChunks).map(i => vec((numPerChunk*i) until (numPerChunk*i + numPerChunk)))
    val splitMat = (0 until numChunks).map(i => mat((numPerChunk*i) until (numPerChunk*i + numPerChunk), ::))

    val linearMapper = new LinearMapper(mat)
    val blockLinearMapper = new BlockLinearMapper(splitMat)

    assert(Stats.aboutEq(blockLinearMapper(splitVec), linearMapper(vec), 1e-4))
  }
}
