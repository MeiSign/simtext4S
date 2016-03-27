package de.simtext

import org.specs2.mutable.Specification

import scala.collection.immutable.SortedMap

class SimTextSpec extends Specification {
  "SimText4s" should {
    val text1 = List("dies", "ist", "der", "inhalt", "von", "text1")
    val text2 = List("dies", "ist", "der", "inhalt", "von", "text2", "dies", "ist", "nicht", "der", "inhalt", "von", "text1")
    val text3 = List("dies", "ist", "der", "inhalt", "von", "text1", "mit", "ein", "paar", "extras")

    "build correct forward reference tables" in {
      val fwdRefTable = SimText.buildForwardReferenceTable(text1 ++ text2, 3)
      fwdRefTable must be equalTo SortedMap(0 -> 6, 1 -> 7, 2 -> 8, 3 -> 16, 8 -> 15)
    }

    "compare tokenlists correctly" in {
      val fwdRefTable = SimText.buildForwardReferenceTable(text1 ++ text2, 3)
      val similarity = SimText.compareTokenLists(text1.length, fwdRefTable, 3)

      similarity must be equalTo 100.0
    }

    "compare tokenlists correctly" in {
      val fwdRefTable = SimText.buildForwardReferenceTable(text3 ++ text2, 3)
      val similarity = SimText.compareTokenLists(text3.length, fwdRefTable, 3)

      similarity must be equalTo 60.0
    }
  }
}
