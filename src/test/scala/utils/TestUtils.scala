package utils

import java.io.ByteArrayInputStream
import org.apache.commons.io.IOUtils

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
   * @param pathInTestResources Input path.
   * @return Resource URI.
   */
  def getTestResourceFileName(pathInTestResources: String): String = {
    getClass.getClassLoader.getResource(pathInTestResources).getFile
  }
}
