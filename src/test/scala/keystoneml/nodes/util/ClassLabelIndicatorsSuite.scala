package keystoneml.nodes.util

import breeze.linalg.DenseVector
import org.scalatest.FunSuite

class ClassLabelIndicatorsSuite extends FunSuite {
  test("single label indicators") {
    intercept[AssertionError] {
      val zerolabels = ClassLabelIndicatorsFromIntLabels(0)
    }

    intercept[AssertionError] {
      val onelabel = ClassLabelIndicatorsFromIntLabels(1)
    }


    val fivelabel = ClassLabelIndicatorsFromIntLabels(5)
    assert(fivelabel(2) === DenseVector(-1.0,-1.0,1.0,-1.0,-1.0))

    intercept[RuntimeException] {
      fivelabel(5)
    }
  }

  test("multiple label indicators without validation") {
    intercept[AssertionError] {
      val zerolabels = ClassLabelIndicatorsFromIntArrayLabels(0)
    }

    intercept[AssertionError] {
      val onelabel = ClassLabelIndicatorsFromIntArrayLabels(1)
    }

    val fivelabel = ClassLabelIndicatorsFromIntArrayLabels(5)

    assert(fivelabel(Array(2,1)) === DenseVector(-1.0,1.0,1.0,-1.0,-1.0))

    intercept[IndexOutOfBoundsException] {
      fivelabel(Array(4,6))
    }

    assert(fivelabel(Array(-1,2)) === DenseVector(-1.0,-1.0,1.0,-1.0,1.0),
      "In the unchecked case, we should get weird behavior.")

  }

  test("multiple label indicators with validation") {
    intercept[AssertionError] {
      val zerolabels = ClassLabelIndicatorsFromIntArrayLabels(0, true)
    }

    intercept[AssertionError] {
      val onelabel = ClassLabelIndicatorsFromIntArrayLabels(1, true)
    }

    val fivelabel = ClassLabelIndicatorsFromIntArrayLabels(5, true)

    assert(fivelabel(Array(2,1)) === DenseVector(-1.0,1.0,1.0,-1.0,-1.0))

    intercept[RuntimeException] {
      fivelabel(Array(4,6))
    }

    intercept[RuntimeException] {
      fivelabel(Array(-1,2))
    }
  }
}
