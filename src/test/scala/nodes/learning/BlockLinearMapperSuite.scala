package nodes.learning

import breeze.linalg.{DenseVector, DenseMatrix}
import breeze.stats.distributions.Rand
import workflow.PipelineContext
import scala.collection.mutable.ArrayBuffer

import org.scalatest.FunSuite

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import pipelines._
import utils.Stats

class BlockLinearMapperSuite extends FunSuite with PipelineContext with Logging {

  test("BlockLinearMapper transformation") {
    sc = new SparkContext("local", "test")

    val inDims = 1000
    val outDims = 100
    val numChunks = 5
    val numPerChunk = inDims/numChunks

    val mat = DenseMatrix.rand(inDims, outDims, Rand.gaussian)
    val vec = DenseVector.rand(inDims, Rand.gaussian)
    val intercept = DenseVector.rand(outDims, Rand.gaussian)

    val splitVec = (0 until numChunks).map(i => vec((numPerChunk*i) until (numPerChunk*i + numPerChunk)))
    val splitMat = (0 until numChunks).map(i => mat((numPerChunk*i) until (numPerChunk*i + numPerChunk), ::))

    val linearMapper = new LinearMapper[DenseVector[Double]](mat, Some(intercept))
    val blockLinearMapper = new BlockLinearMapper(splitMat, numPerChunk, Some(intercept))

    val linearOut = linearMapper(vec)

    // Test with intercept
    assert(Stats.aboutEq(blockLinearMapper(vec), linearOut, 1e-4))

    // Test the apply and evaluate call
    val blmOuts = new ArrayBuffer[RDD[DenseVector[Double]]]
    val splitVecRDDs = splitVec.map { vec =>
      sc.parallelize(Seq(vec), 1)
    }
    blockLinearMapper.applyAndEvaluate(splitVecRDDs,
      (predictedValues: RDD[DenseVector[Double]]) => {
        blmOuts += predictedValues
        ()
      }
    )

    // The last blmOut should match the linear mapper's output
    assert(Stats.aboutEq(blmOuts.last.collect()(0), linearOut, 1e-4))
  }
}
