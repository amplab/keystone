package utils

import java.io._
import pipelines.Transformer

/**
 * Provides facilities for saving and loading a pipeline to and from disk.
 *
 * This feature is *VERY* experimental and will likely change in the future.
 * In its current implementation, pipelines saved with one version of Keystone will likely
 * be incompatible with future versions.
 *
 * TODO: This doesn't handle saving and loading huge objects very well.
 * Also, it blindly assumes that the input object is
 * serializable.
 */
object PipelinePersistence {
  def savePipeline[T,U](pipe: Transformer[T,U], file: File) = {
    //Serialize the pipeline to disk.
    val writer = new ObjectOutputStream(new FileOutputStream(file))
    writer.writeObject(pipe)
    writer.close()
  }

  def loadPipeline[T,U](file: File): Transformer[T,U] = {
    val inp = new ObjectInputStream(new FileInputStream(file))
    val pipeline = inp.readObject().asInstanceOf[Transformer[T,U]]
    inp.close()
    pipeline
  }
}