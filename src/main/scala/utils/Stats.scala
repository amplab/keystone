package utils

import java.io.{FileWriter, File}
import java.util.{Random => JRandom}
import javax.imageio.ImageIO

import org.apache.commons.io.FileUtils
import pipelines._
import org.apache.spark.rdd.RDD
import breeze.linalg._
import breeze.numerics._
import breeze.stats._

import scala.reflect.ClassTag

object Stats {

  def rand(rows: Int, cols: Int, r: JRandom = new java.util.Random()): DenseMatrix[DataType] = {
    val m = new DenseMatrix[DataType](rows, cols)
    val r = new java.util.Random()
    var i = 0
    while (i < rows * cols) {
      m.data(i) = r.nextDouble()
      i = i + 1
    }
    m
  }

  def randn(rows: Int, cols: Int, r: JRandom = new java.util.Random() ): DenseMatrix[DataType] = {
    val m = new DenseMatrix[DataType](rows, cols)
    var i = 0
    while (i < rows * cols) {
      m.data(i) = r.nextGaussian()
      i = i + 1
    }
    m
  }

  def randCauchy(rows: Int, cols: Int, r: JRandom = new java.util.Random()): DenseMatrix[DataType] = {
    val m = new DenseMatrix[DataType](rows, cols)
    var i = 0
    while (i < rows * cols) {
      // Get a random number betwee -Pi/2 to Pi/2
      val angle = (r.nextDouble() - 1.0) * math.Pi / 2
      m.data(i) = math.tan(angle)
      i = i + 1
    }
    m
  }
}
