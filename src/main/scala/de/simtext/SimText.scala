package de.simtext

import java.io.File

import com.typesafe.scalalogging.Logger
import de.simtext.domain.{CompareResult, CompareTuple}
import org.slf4j.LoggerFactory

object SimText extends App {
  val logger = Logger(LoggerFactory.getLogger("name"))
  val sliding_step = 3

  val folder = new File(args(0))
  require(folder.isDirectory, s"${folder.getAbsolutePath} must be a directory")

  val files = recursiveListFiles(folder)
  val tokenizer = new Tokenizer()

  files.foreach { file1 =>
    files.toList.foreach { file2 =>
      val compareTuple = CompareTuple(file1, file2, tokenizer, sliding_step)
      val compareResult = compareTokenLists(compareTuple)
      logger.info(s"${compareResult.name1} and ${compareResult.name2} similarity is: ${compareResult.similarity}")
    }
  }

  def compareTokenLists(compareTuple: CompareTuple): CompareResult = {
      val indices: List[Int] = compareTuple.forwardReferenceTable.map {
        case (key, value) =>
          if (key < compareTuple.tokens1.length && matchInTarget(key, compareTuple.tokens1.length, compareTuple.forwardReferenceTable)) key
          else -1
      }.toList.filter(_ != -1).sorted

      val sum = indices.sliding(2).foldLeft(sliding_step) {
        case (acc, a :: b :: rest) => if ((b - a) < sliding_step) acc + (b - a) else acc + sliding_step
        case (acc, _) => acc
      }

      CompareResult(compareTuple.file1.getName, compareTuple.file2.getName, sum * 100.0 / compareTuple.tokens1.length)
  }

  private def matchInTarget(key: Int, targetStart: Int, forwardReferenceTable: Map[Int, Int]): Boolean = {
    forwardReferenceTable.get(key).fold(false){ value =>
      if (value >= targetStart) true
      else matchInTarget(value, targetStart, forwardReferenceTable)
    }
  }

  def recursiveListFiles(f: File): Array[File] = {
    val these = f.listFiles
    these ++ these.filter(_.isDirectory).flatMap(recursiveListFiles)
  }
}