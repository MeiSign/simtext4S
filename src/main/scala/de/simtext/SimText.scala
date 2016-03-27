package de.simtext

import de.simtext.domain.{CompareResult, CompareTuple, TokenizedText}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.SortedMap
import scala.collection.immutable.HashMap

object SimText {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("simtext4s with Spark")
    val sc = new SparkContext(conf)

    val minMatchLength = 3
    val tokenzier = new Tokenizer()

    val tokenizedFiles: RDD[(String, List[String])] = sc
      .wholeTextFiles("hdfs://hadoop03.f4.htw-berlin.de:8020/studenten/s0531927/Muenster-Med/", 300)
      .map {
        case (name, text) => (name, tokenzier.tokenize(text))
      }

    val comparetuples = tokenizedFiles.cartesian(tokenizedFiles).map {
      case ((name1, tokens1), (name2, tokens2)) =>
        CompareTuple(TokenizedText(name1, tokens1), TokenizedText(name2, tokens2))
    }

    val results = comparetuples.map { compareTuple =>
      val forwardRefTable: SortedMap[Int, Int] = buildForwardReferenceTable(compareTuple.combinedTokens, minMatchLength)
      val similarity = compareTokenLists(compareTuple.splitIndex, forwardRefTable, minMatchLength)
      CompareResult(compareTuple.tokenizedText1.name, compareTuple.tokenizedText2.name, similarity)
    }

    results.saveAsTextFile("hdfs://hadoop03.f4.htw-berlin.de:8020/studenten/s0531927/samples/result")

    sc.stop()
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
