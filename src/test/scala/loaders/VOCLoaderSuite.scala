package loaders

import org.scalatest.FunSuite
import org.apache.spark.SparkContext
import utils.TestUtils
import workflow.PipelineContext

class VOCLoaderSuite extends FunSuite with PipelineContext {
  test("load a sample of VOC data") {
    sc = new SparkContext("local", "test")
    val dataPath = TestUtils.getTestResourceFileName("images/voc")
    val labelsPath = TestUtils.getTestResourceFileName("images/voclabels.csv")

    val imgs = VOCLoader(sc,
      VOCDataPath(dataPath, "VOCdevkit/VOC2007/JPEGImages/", Some(1)),
      VOCLabelPath(labelsPath)).collect()

    // We should have 10 images
    assert(imgs.length === 10)

    // There should be one file whose name ends with "000104.jpg"
    val personMonitor = imgs.filter(_.filename.get.endsWith("000104.jpg"))
    assert(personMonitor.length === 1)

    // It should have two labels, 14 and 19.
    assert(personMonitor(0).label.contains(14) && personMonitor(0).label.contains(19))

    // There should be two 13 labels total and 9 should be distinct.
    assert(imgs.map(_.label).flatten.length === 13)
    assert(imgs.map(_.label).flatten.distinct.length === 9)
  }
}
