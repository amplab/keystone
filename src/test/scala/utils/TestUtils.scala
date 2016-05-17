package utils

import java.io.{FileReader, ByteArrayInputStream}
import breeze.linalg.DenseMatrix
import breeze.stats.distributions.{Gaussian, RandBasis, ThreadLocalRandomGenerator, Rand}
import edu.berkeley.cs.amplab.mlmatrix.RowPartitionedMatrix
import org.apache.commons.io.IOUtils
import org.apache.commons.math3.random.MersenneTwister
import org.apache.spark.SparkContext

import scala.io.Source
import scala.util.Random

/** Some utility methods for pipeline tests. */
object TestUtils {
  /** Load an image named @pathInTestResources, which must live under src/test/resources. */
  def loadTestImage(pathInTestResources: String): Image = {
    val imageUri = getClass.getClassLoader.getResource(pathInTestResources).toURI()
    val bytes = new ByteArrayInputStream(IOUtils.toByteArray(imageUri))
    val image = ImageUtils.loadImage(bytes)
    if (image.isDefined) {
      image.get
    } else {
      throw new Exception("TestUtils.loadImage: No image found at %s.".format(pathInTestResources))
    }
  }

  /**
   * Gets test resource URI for loading.
    *
    * @param pathInTestResources Input path.
   * @return Resource URI.
   */
  def getTestResourceFileName(pathInTestResources: String): String = {
    getClass.getClassLoader.getResource(pathInTestResources).getFile
  }

  /** Load a file named @pathInTestResources, which must live under src/test/resources. */
  def loadFile(pathInTestResources: String): Seq[String] = {
    val fileURI = getClass.getClassLoader.getResource(pathInTestResources).toURI
    Source.fromFile(fileURI).getLines().toSeq
  }

  /** These methods are used to generate random images. */
  def genData(x: Int, y: Int, z: Int, inorder: Boolean=false): Array[Double] = {
    if (!inorder) Array.fill(x*y*z)(Random.nextDouble) else (0 until x*y*z).map(_.toDouble).toArray
  }

  /** Generate a random `RowMajorArrayVectorizedImage` */
  def genRowMajorArrayVectorizedImage(x: Int, y: Int, z: Int): RowMajorArrayVectorizedImage = {
    RowMajorArrayVectorizedImage(genData(x, y, z), ImageMetadata(x,y,z))
  }

  /** Generate a random `ColumnMajorArrayVectorizedImage` */
  def genColumnMajorArrayVectorizedImage(x: Int, y: Int, z: Int): ColumnMajorArrayVectorizedImage = {
    ColumnMajorArrayVectorizedImage(genData(x, y, z), ImageMetadata(x,y,z))
  }

  /** Generate a random `ChannelMajroArrayVectorizedImage` */
  def genChannelMajorArrayVectorizedImage(x: Int, y: Int, z: Int): ChannelMajorArrayVectorizedImage = {
    ChannelMajorArrayVectorizedImage(genData(x, y, z), ImageMetadata(x,y,z))
  }

  def genRowColumnMajorByteArrayVectorizedImage(x: Int, y: Int, z: Int): RowColumnMajorByteArrayVectorizedImage = {
    RowColumnMajorByteArrayVectorizedImage(genData(x,y,z).map(_.toByte), ImageMetadata(x,y,z))
  }

  def createRandomMatrix(
      sc: SparkContext,
      numRows: Int,
      numCols: Int,
      numParts: Int,
      seed: Int = 42): RowPartitionedMatrix = {

    val rowsPerPart = numRows / numParts
    val matrixParts = sc.parallelize(1 to numParts, numParts).mapPartitionsWithIndex { (index, part) =>
      val randBasis: RandBasis = new RandBasis(new ThreadLocalRandomGenerator(new MersenneTwister(seed+index)))
      Iterator(DenseMatrix.rand(rowsPerPart, numCols, Gaussian(0.0, 1.0)(randBasis)))
    }
    RowPartitionedMatrix.fromMatrix(matrixParts.cache())
  }

  def createLocalRandomMatrix(numRows: Int, numCols: Int, seed: Int = 42): DenseMatrix[Double] = {
    val randBasis: RandBasis = new RandBasis(new ThreadLocalRandomGenerator(new MersenneTwister(seed)))
    DenseMatrix.rand(numRows, numCols, Gaussian(0.0, 1.0)(randBasis))
  }
}
