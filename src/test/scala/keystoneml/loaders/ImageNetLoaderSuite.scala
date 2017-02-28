package keystoneml.loaders

import org.scalatest.FunSuite
import org.apache.spark.SparkContext
import keystoneml.utils.TestUtils
import keystoneml.workflow.PipelineContext

class ImageNetLoaderSuite extends FunSuite with PipelineContext {
  test("load a sample of imagenet data") {
    sc = new SparkContext("local", "test")
    val dataPath = TestUtils.getTestResourceFileName("images/imagenet")
    val labelsPath = TestUtils.getTestResourceFileName("images/imagenet-test-labels")

    val imgs = ImageNetLoader.apply(sc, dataPath, labelsPath).collect()
    // We should have 5 images
    assert(imgs.length === 5)

    // The images should all have label 12
    assert(imgs.map(_.label).distinct.length === 1)
    assert(imgs.map(_.label).distinct.head === 12)

    // The image filenames should begin with n15075141
    assert(imgs.forall(_.filename.get.startsWith("n15075141")), "Image filenames should be correct")
  }
}
