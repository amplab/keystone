package keystoneml.nodes.nlp

import org.scalatest.FunSuite

class NGramIndexerSuite extends FunSuite {

  test("pack()") {
    require(NaiveBitPackIndexer.pack(Seq(1)) == math.pow(2, 40).toLong)

    require(NaiveBitPackIndexer.pack(Seq(1, 1)) ==
      math.pow(2, 40).toLong + math.pow(2, 20).toLong + math.pow(2, 60).toLong)

    require(NaiveBitPackIndexer.pack(Seq(1, 1, 1)) ==
      1 + math.pow(2, 40).toLong + math.pow(2, 20).toLong + math.pow(2, 61).toLong)

    val ngramIndexer = new NGramIndexerImpl[Int]
    val seq = ngramIndexer.minNgramOrder to ngramIndexer.maxNgramOrder
    require(ngramIndexer.pack(seq).equals(new NGram(seq)))
  }

  test("removeFarthestWord()") {
    def testWith[Word >: Int, Ngram](indexer: BackoffIndexer[Word, Ngram]) = {
      var ngramId = indexer.pack(Seq(1, 2, 3))
      var context = indexer.removeFarthestWord(ngramId)
      var expected = indexer.pack(Seq(2, 3))
      require(context == expected, s"actual $context, expected $expected")

      ngramId = indexer.pack(Seq(1, 2))
      context = indexer.removeFarthestWord(ngramId)
      expected = indexer.pack(Seq(2))
      require(context == expected, s"actual $context, expected $expected")
    }

    testWith(new NGramIndexerImpl[Int])
    testWith(NaiveBitPackIndexer)
  }

  test("removeCurrentWord()") {
    def testWith[Word >: Int, Ngram](indexer: BackoffIndexer[Word, Ngram]) = {
      var ngramId = indexer.pack(Seq(1, 2, 3))
      var context = indexer.removeCurrentWord(ngramId)
      var expected = indexer.pack(Seq(1, 2))
      require(context == expected, s"actual $context, expected $expected")

      ngramId = indexer.pack(Seq(1, 2))
      context = indexer.removeCurrentWord(ngramId)
      expected = indexer.pack(Seq(1))
      require(context == expected, s"actual $context, expected $expected")
    }

    testWith(new NGramIndexerImpl[Int])
    testWith(NaiveBitPackIndexer)
  }

}
