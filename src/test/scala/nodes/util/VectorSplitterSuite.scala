package nodes.util

import breeze.linalg._
import org.scalatest.FunSuite

class VectorSplitterSuite extends FunSuite {
  test("vector splitter") {
    for (
      bs <- Array(128, 256, 512, 1024, 2048);
      mul <- 0 to 2;
      off <- 0 to 20 by 5;
      feats <- Array(Some(bs*mul + off), None)
    ) {
      val sp = new VectorSplitter(bs, feats)
      val vec = DenseVector.zeros[Double](bs*mul + off)

      val expectedSplits = (bs*mul + off)/bs + (if ((bs*mul + off) % bs == 0) 0 else 1)

      assert(sp.splitVector(vec).length === expectedSplits,
        s"True length is ${sp.splitVector(vec).length}, expected length is ${expectedSplits}")
    }
  }

  test("vector splitter maintains order") {
    for (
      bs <- Array(128, 256, 512, 1024, 2048);
      mul <- 0 to 2;
      off <- 0 to 20 by 5;
      feats <- Array(Some(bs*mul + off), None)
    ) {
      val sp = new VectorSplitter(bs, feats)
      val vec = rand(bs*mul + off)

      val expectedSplits = (bs*mul + off) / bs + (if ((bs*mul+off) % bs == 0) 0 else 1)

      assert(DenseVector.vertcat(sp.splitVector(vec):_*) === vec,
        s"Recombinded split vector of length ${bs*mul + off} with block size $bs did not match its input")
    }
  }
}