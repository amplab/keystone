package pipelines

import org.apache.spark.{SparkConf, SparkContext}


/**
 * An abstract class that provides driver methods and common configuration parameters to pipelines.
 */
trait PipelineApp[C] {
  /**
   * Name of the application.
   */
  val appName: String

  /**
   * Run must take in a context and a configuration and produces a Transformer,
   * or a Pipeline that can be run on new data.
   * @param sc
   * @param conf
   * @return
   */
  def run(sc: SparkContext, conf: C): Transformer[_,_]

  /**
   * Parse is a method that performs argument parsing. Usually handled via scopt.
   * @param args
   * @return
   */
  def parse(args: Array[String]): C

  /**
   * The actual driver receives its configuration parameters from spark-submit usually.
   * @param args
   */
  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName(appName)
    conf.setIfMissing("master", "local[2]") // This is a fallback if things aren't set via spark submit.

    val sc = new SparkContext(conf)

    val appConfig = parse(args)
    run(sc, appConfig)

    sc.stop()
  }
}