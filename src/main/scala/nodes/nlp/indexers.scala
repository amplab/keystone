package nodes.nlp

import scala.reflect.ClassTag

trait NGramIndexer[T, U] extends Serializable {

  type WordType = T
  type NgramType = U

  val minNgramOrder: Int
  val maxNgramOrder: Int

  /**
   * Packs a sequence of word IDs of type U into a single U.  The current word
   * is `ngram.last`, and the words before are the context.
   */
  def pack(ngram: Seq[WordType]): NgramType
}

/**
 * A family of NGramIndexer that can unpack or strip off specific words, query
 * the order of an packed ngram, etc.
 *
 * Such indexers are useful for LMs that require backoff contexts (e.g. Stupid Backoff, KN).
 */
trait BackoffIndexer[T, U] extends NGramIndexer[T, U] {

  /**
   * Unpacks the `pos` word out of the packed ngram of type U.  Position 0
   * indicates the farthest context (if unigram, the current word), and position
   * MAX_ORDER-1 represents the current word.
   *
   * Useful for getting words at special positions (e.g. first two in context).
   */
  def unpack(ngram: NgramType, pos: Int): WordType

  def removeFarthestWord(ngram: NgramType): NgramType

  def removeCurrentWord(ngram: NgramType): NgramType

  /** Returns an order in [minNgramOrder, maxNgramOrder] if valid; otherwise errors out. */
  def ngramOrder(ngram: NgramType): Int

}

/**
 * Packs up to 3 words (trigrams) into a single Long by bit packing.
 *
 * Assumptions:
 * (1) |Vocab| <= one million (20 bits per word).
 * (2) Words get mapped into [0, |Vocab|).  In particular, each word ID < 2**20.
 */
object NaiveBitPackIndexer extends BackoffIndexer[Int, Long] {

  final val minNgramOrder = 1
  final val maxNgramOrder = 3

  /**
   * The packed layout (most significant to least):
   *     [4 control bits] [farthest word] ... [curr word].
   * If can't fill all bits, we prefer to left-align.
   */
  def pack(ngram: Seq[Int]): Long = {
    ngram.foreach { word => require(word < math.pow(2, 20)) }
    // Four most significant bits are control bits:
    // 0000: unigram; 0001: bigram; 0010: trigram
    ngram.length match {
      case 1 => ngram(0).toLong << 40
      case 2 => (ngram(1).toLong << 20) | (ngram(0).toLong << 40) | (1L << 60)
      case 3 => ngram(2).toLong | (ngram(1).toLong << 20) | (ngram(0).toLong << 40) | (1L << 61)
      case _ => sys.error("ngram order need to be in { 1, 2, 3 } for now")
    }
  }

  def unpack(ngram: Long, pos: Int): Int = pos match {
    case 0 => ((ngram >>> 40) & ((1 << 20) - 1)).asInstanceOf[Int]
    case 1 => ((ngram >>> 20) & ((1 << 20) - 1)).asInstanceOf[Int]
    case 2 => (ngram & ((1 << 20) - 1)).asInstanceOf[Int]
    case _ => sys.error("ngram order need to be in { 1, 2, 3 } for now")
  }

  def removeFarthestWord(ngram: Long): Long = {
    val order = ngramOrder(ngram)
    val ngramCleared = ngram & (0xF << 60L)
    val stripped = ngram & ((1L << 40) - 1)
    val shifted = ((stripped << 20L) | ngramCleared) & ~(0xF << 60L)
    // Now set the control bits accordingly
    order match {
      case 2 => shifted
      case 3 => shifted | (1L << 60)
      case _ => sys.error(s"ngram order is either invalid or not supported: $order")
    }
  }

  def removeCurrentWord(ngram: Long): Long = {
    val order = ngramOrder(ngram)
    order match {
      case 2 =>
        val stripped = ngram & ~((1L << 40) - 1)
        stripped & ~(0xF << 60L)
      case 3 =>
        val stripped = ngram & ~((1L << 20) - 1)
        (stripped & ~(0xF << 60L)) | (1L << 60)
      case _ => sys.error(s"ngram order is either invalid or not supported: $order")
    }
  }

  def ngramOrder(ngram: Long): Int = {
    val order = ((ngram & (0xF << 60L)) >>> 60).asInstanceOf[Int]
    if (order + 1 >= minNgramOrder && order + 1 <= maxNgramOrder) {
      order + 1
    } else {
      sys.error(s"raw control bits $order are invalid")
    }
  }

}

class NGramIndexerImpl[@specialized(Int) T: ClassTag]
  extends BackoffIndexer[T, NGram[T]] {

  final val minNgramOrder = 1
  final val maxNgramOrder = 5 // TODO: makes sense to set it to infty?

  // TODO: Call .toArray() to do a copy?
  def pack(ngram: Seq[T]): NGram[T] = new NGram(ngram)

  def unpack(ngram: NGram[T], pos: Int): T = ngram.words(pos)

  // TODO: does the interface allow modifying same NGram object?
  def removeFarthestWord(ngram: NGram[T]): NGram[T] =
    new NGram(ngram.words.drop(1))

  def removeCurrentWord(ngram: NGram[T]): NGram[T] =
    new NGram(ngram.words.dropRight(1))

  def ngramOrder(ngram: NGram[T]): Int = ngram.words.length

}
