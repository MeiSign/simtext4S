package de.simtext

import de.simtext.domain.CompareTuple
import scala.collection.mutable
import scala.concurrent.Future
import scala.io.Source
import scala.concurrent.ExecutionContext.Implicits.global

object SimText extends App {
  val sliding_step = 3

  val file1 = Source.fromFile(args(0))
  val text1 = try file1.mkString finally file1.close()

  val file2 = Source.fromFile(args(1))
  val text2 = try file2.mkString finally file1.close()

  val tokenizer = new Tokenizer()
  val compare = CompareTuple(text1, text2, tokenizer)

  val percentages = compareTokenLists(compare.tokens1, compare.tokens2)

  percentages.map(println)


  def compareTokenLists(tokens1: List[String], tokens2: List[String]): Future[Double] = {
    getEqualPercentage(tokens1, tokens2)
  }

  private def getEqualPercentage(tokens1: List[String], tokens2: List[String]): Future[Double] = {
    Future {
      var matches = new mutable.HashMap[String, Boolean]

      val indices = compare.tokens1.sliding(sliding_step).zipWithIndex.map {
        case (slice, matchStartingIndex) =>
          if (matches.getOrElse(slice.toString(), false)) matchStartingIndex
          else {
            if (compare.tokens2.indexOfSlice(slice) == -1) -1
            else {
              matches += (slice.toString -> true)
              matchStartingIndex
            }
          }
      }.filter(_ != -1)

      val sum = indices.sliding(2).foldLeft(sliding_step) {
        case (acc, a :: b :: rest) => if ((b - a) < sliding_step) acc + (b - a) else acc + sliding_step
        case (acc, _) => acc
      }

      sum * 100.0 / tokens1.length
    }
  }
}