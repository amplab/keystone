package nodes.nlp

import org.scalatest.FunSuite
import pipelines.LocalSparkContext

class HashingTFSuite extends FunSuite with LocalSparkContext {

  test("HashingTF with no collisions") {
    val dims = 4000
    val hashingTF = HashingTF[Seq[String]](dims)

    val testDatum = Seq("1", "2", "4", "4", "4", "4", "2")

    val vector = hashingTF(testDatum)

    // Assert that the vector is actually sparse and has the right number of active positions
    assert(vector.activeSize === 3)
    assert(vector.length === dims)

    val termFrequenciesSet = vector.toArray.toSet

    // Assert that there are indices with all of the correct values
    assert(termFrequenciesSet === Set(0, 1, 2, 4))
  }

  test("HashingTF with collisions") {
    val hashingTF = HashingTF[Seq[String]](2)

    val testDatum = Seq("1", "2", "4", "4", "4", "4", "2")

    val vector = hashingTF(testDatum)
    assert(vector.activeSize === 2)
    assert(vector.length === 2)

    // Assert that the sum of the tf's is still correct even though there were collisions
    assert(vector.toArray.sum === testDatum.size)
  }
}
