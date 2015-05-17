package utils

import java.io.File

import org.scalatest.FunSuite
import pipelines.{Logging, Transformer}
import utils.PipelinePersistence._

class A extends Transformer[Int,Int] {
  def apply(a: Int): Int = a + 5
}
class B extends Transformer[Int, String] {
  def apply(a: Int): String = a.toString + "yeah"
}

class PipelinePersistenceSuite extends FunSuite with Logging {
  val pipe = new A() then new B()

  val tmpFile = File.createTempFile("test", "obj")

  test("Testing saving.") {
    savePipeline(pipe, tmpFile)
  }

  test("Testing loading.") {
    val b = loadPipeline[Int,String](tmpFile)
    assert(b(5) == "10yeah")
  }

  tmpFile.delete()
}