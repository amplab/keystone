package keystoneml.nodes.stats

import breeze.linalg.DenseVector
import org.scalatest.FunSuite

class SignedHellingerMapperSuite extends FunSuite {
  test("signed hellinger mapper") {
    val x = DenseVector(1.0, -4.0, 0.0, -9.0, 16.0)
    val shmx = DenseVector(1.0, -2.0, 0.0, -3.0, 4.0)

    assert(SignedHellingerMapper(x) == shmx, "Result should be signed square root of input.")
  }
}
