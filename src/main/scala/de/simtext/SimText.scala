package de.simtext

import de.simtext.domain.{CompareResult, CompareTuple, TokenizedText}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.SortedMap
import scala.collection.immutable.HashMap
import com.typesafe.config.{Config, ConfigFactory}

object SimText {

  val conf = ConfigFactory.load()
  val partitionsCount = conf.getInt("simtext.partitions")
  val minMatchLength = conf.getInt("simtext.minmatchlength")
  val hdfsDir1 = conf.getString("simtext.hdfs.dir1")
  val hdfsDir2 = conf.getString("simtext.hdfs.dir2")
  val outputDir = conf.getString("simtext.hdfs.output")

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("simtext4s with Spark")
    val sc = new SparkContext(conf)

    val tokenizer = new Tokenizer()

    val tokenizedFiles1 = getTokenizedFilesAsRdd(sc, tokenizer, hdfsDir1)
    val tokenizedFiles2 = getTokenizedFilesAsRdd(sc, tokenizer, hdfsDir2)

    val comparetuples = tokenizedFiles1.cartesian(tokenizedFiles2).map {
      case ((name1, tokens1), (name2, tokens2)) =>
        CompareTuple(TokenizedText(name1, tokens1), TokenizedText(name2, tokens2))
    }

    val results = comparetuples.map { compareTuple =>
      val forwardRefTable: SortedMap[Int, Int] = buildForwardReferenceTable(compareTuple.combinedTokens, minMatchLength)
      val similarity = compareTokenLists(compareTuple.splitIndex, forwardRefTable, minMatchLength)
      CompareResult(compareTuple.tokenizedText1.name, compareTuple.tokenizedText2.name, similarity)
    }

    results.saveAsTextFile(outputDir)

    sc.stop()
  }

  private def getTokenizedFilesAsRdd(sc: SparkContext, tokenizer: Tokenizer, path: String) = {
    sc.wholeTextFiles(path, partitionsCount).map {
        case (name, text) => (name, tokenizer.tokenize(text))
      }
  }

  private def buildForwardReferenceTable(tokens: List[String], minMatchLength: Int): SortedMap[Int, Int] = {
    val (_, forwardRefTable) = tokens
      .sliding(minMatchLength)
      .zipWithIndex
      .foldLeft(HashMap.empty[List[String], Int], SortedMap.empty[Int, Int]) {
        case ((lastIndexAcc, forwardRefTableAcc), (slice, index: Int)) =>
        if (lastIndexAcc.contains(slice)) {
          val updatedForwardRefTable = forwardRefTableAcc + (lastIndexAcc.get(slice).get -> index)
          val updatedLastIndexAcc = lastIndexAcc + (slice -> index)
          (updatedLastIndexAcc, updatedForwardRefTable)
        } else {
          val updatedLastIndexAcc = lastIndexAcc + (slice -> index)
          (updatedLastIndexAcc, forwardRefTableAcc)
        }
    }

    forwardRefTable
  }

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

  private def matchInTarget(key: Int, targetStart: Int, forwardReferenceTable: SortedMap[Int, Int]): Boolean = {
    forwardReferenceTable.get(key).fold(false){ value =>
      if (value >= targetStart) true
      else matchInTarget(value, targetStart, forwardReferenceTable)
    }
  }
}
