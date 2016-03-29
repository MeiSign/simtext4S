package de.simtext

import com.typesafe.config.ConfigFactory
import de.simtext.domain.{CompareResult, TokenizedText, CompareTuple}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.SortedMap
import scala.collection.immutable.HashMap

object SimText {

  // initializing config from resources/application.conf
  val conf = ConfigFactory.load()
  val partitionsCount = conf.getInt("simtext.partitions")
  val minMatchLength = conf.getInt("simtext.minmatchlength")
  val simThreshhold = conf.getInt("simtext.similaritythreshhold")
  val hdfsDir1 = conf.getString("simtext.hdfs.dir1")
  val hdfsDir2 = conf.getString("simtext.hdfs.dir2")
  val outputDir = conf.getString("simtext.hdfs.output.dir")
  val outputPartitions = conf.getInt("simtext.hdfs.output.partitions")
  // initializing tokenizer options
  val ignoreLetterCase = conf.getBoolean("simtext.tokenizer.ignore.lettercase")
  val ignoreNumbers = conf.getBoolean("simtext.tokenizer.ignore.numbers")
  val ignorePunctuation = conf.getBoolean("simtext.tokenizer.ignore.punctuation")
  val ignoreUmlauts = conf.getBoolean("simtext.tokenizer.ignore.umlauts")

  /**
    * the main method runs the spark job and gets called by the spark system
    * First it loads all necessary textfiles and generates the cartesian product
    * of them to create compare tuples.
    * afterwards it generates a forward reference table for each tuple and compares
    * the textfiles with each other.
    * the results are written into hdfs.
    *
    * @param args additional spark parameters which are passed by the spark system
    */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("simtext4s with Spark")
    val sc = new SparkContext(conf)

    val tokenizer = new Tokenizer(
      ignoreLetterCase = ignoreLetterCase,
      ignoreNumbers = ignoreNumbers,
      ignorePunctuation = ignorePunctuation,
      ignoreUmlauts = ignoreUmlauts
    )

    val tokenizedFiles1 = getTokenizedFilesAsRdd(sc, tokenizer, hdfsDir1)
    val tokenizedFiles2 = getTokenizedFilesAsRdd(sc, tokenizer, hdfsDir2)

    val comparetuples: RDD[CompareTuple] = tokenizedFiles1.cartesian(tokenizedFiles2).map {
      case ((name1, tokens1), (name2, tokens2)) =>
        CompareTuple(TokenizedText(name1, tokens1), TokenizedText(name2, tokens2))
    }

    val results: RDD[CompareResult] = comparetuples.map { compareTuple =>
      val forwardRefTable: SortedMap[Int, Int] = buildForwardReferenceTable(
        compareTuple.combinedTokens,
        minMatchLength
      )
      val similarity: Double = compareTokenLists(
        compareTuple.splitIndex,
        forwardRefTable,
        minMatchLength
      )
      CompareResult(compareTuple.tokenizedText1.name, compareTuple.tokenizedText2.name, similarity)
    }.filter(res => res.similarity >= simThreshhold)

    results.coalesce(outputPartitions, shuffle = true).saveAsTextFile(outputDir)

    sc.stop()
  }

  /**
    * Loads files from hdfs as key value pairs (name, text) and
    * tokenizes the text with the provided tokenizer
    *
    * @param sc SparkContext
    * @param tokenizer Tokenizer to preprocess the textfiles
    * @param path Location of the files that shall be loaded
    * @return RDD of the key value pairs (name, text)
    */
  private def getTokenizedFilesAsRdd(sc: SparkContext, tokenizer: Tokenizer, path: String) = {
    sc.wholeTextFiles(path, partitionsCount).map {
        case (name, text) => (name, tokenizer.tokenize(text))
      }
  }

  /**
    * Builds a forward reference table of the provided tokenlist
    * ie.:
    * List (to, be, or, not, to be, to, be, or, not)
    * FwdRefTable: (0 -> 4, 1 -> 8)
    *
    * @param tokens concatenated tokenlist of two textfiles
    * @param minMatchLength minimum length of tokensequence to count as duplicate
    * @return forward reference table of type SortedMap[Int, Int]
    */
  def buildForwardReferenceTable(tokens: List[String], minMatchLength: Int) = {
    val tokSequences = tokens.sliding(minMatchLength)
    val zippedTokenSeq = tokSequences.zipWithIndex
    val (_, forwardRefTable) = zippedTokenSeq
      .foldLeft(HashMap[List[String], Int](), SortedMap[Int, Int]()) {
        case ((lastIdxAcc, fwdRefTableAcc), (slice, index: Int)) =>
          if (lastIdxAcc.contains(slice)) {
            val updatedForwardRefTable = fwdRefTableAcc + (lastIdxAcc.get(slice).get -> index)
            val updatedLastIndexAcc = lastIdxAcc + (slice -> index)
            (updatedLastIndexAcc, updatedForwardRefTable)
          } else {
            val updatedLastIndexAcc = lastIdxAcc + (slice -> index)
            (updatedLastIndexAcc, fwdRefTableAcc)
          }
      }

    forwardRefTable
  }

  /**
    * Takes the splitIndex that marks the end of text1 and start of text2 and counts
    * the percentage of similarity based on the forwardreference table
    *
    * @param splitIndex index where text1 ends and text2 starts
    * @param forwardRefTable forwardreference table with indices of duplicated tokensequences
    * @param minMatchLength minimum length of tokensequence to count as duplicate
    * @return percentage of duplicated tokens from text2 in text1
    */
  def compareTokenLists(splitIndex: Int, forwardRefTable: SortedMap[Int, Int], minMatchLength: Int): Double = {
    val indices: List[Int] = forwardRefTable.map {
      case (key, value) =>
        if (key < splitIndex && matchInTarget(key, splitIndex, forwardRefTable)) key
        else -1
    }.toList.filter(_ != -1)

    val sum = indices.sliding(2).foldLeft(minMatchLength) {
      case (acc, a :: b :: rest) => if ((b - a) < minMatchLength) acc + (b - a) else acc + minMatchLength
      case (acc, _) => acc
    }

    sum * 100.0 / splitIndex
  }

  /**
    * recursive method to see if a tokensequence is present in both texts
    *
    * @param key tokensequence to look for
    * @param targetStart offset index
    * @param forwardReferenceTable forwardreference table
    * @return True if the tokensequence is present in both texts
    */
  private def matchInTarget(key: Int, targetStart: Int, forwardReferenceTable: SortedMap[Int, Int]): Boolean = {
    forwardReferenceTable.get(key).fold(false){ value =>
      if (value >= targetStart) true
      else matchInTarget(value, targetStart, forwardReferenceTable)
    }
  }
}
