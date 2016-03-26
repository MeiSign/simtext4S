package de.simtext.domain

import java.io.File

import de.simtext.Tokenizer

import scala.collection.immutable.HashMap
import scala.io.Source

case class CompareTuple(file1: File, file2: File, tokenizer: Tokenizer, minMatchLength: Int) {
  private val source1 = Source.fromFile(file1)
  private val source2 = Source.fromFile(file2)
  private val text1 = try source1.mkString finally source1.close()
  private val text2 = try source2.mkString finally source2.close()
  val tokens1 = tokenizer.tokenize(text1)
  val tokens2 = tokenizer.tokenize(text2)

  val forwardReferenceTable: Map[Int, Int] = buildForwardReferenceTable(tokens1 ++ tokens2)

  private def buildForwardReferenceTable(tokens: List[String]): Map[Int, Int] = {
    val (_, forwardRefTable) = tokens.sliding(minMatchLength).zipWithIndex.foldLeft(HashMap.empty[String, Int], HashMap.empty[Int, Int]) {
      case ((lastIndexAcc: HashMap[String, Int], forwardRefTableAcc: HashMap[Int, Int]), (slice: List[String], index: Int)) =>
        if (lastIndexAcc.contains(slice.toString())) {
          val updatedForwardRefTable = forwardRefTableAcc + (lastIndexAcc.get(slice.toString()).get -> index)
          val updatedLastIndexAcc = lastIndexAcc + (slice.toString() -> index)
          (updatedLastIndexAcc, updatedForwardRefTable)
        } else {
          val updatedLastIndexAcc = lastIndexAcc + (slice.toString() -> index)
          (updatedLastIndexAcc, forwardRefTableAcc)
        }
    }

    forwardRefTable
  }
}
