package de.simtext

import java.util.Locale

class Tokenizer(ignoreLetterCase: Boolean = true, ignoreNumbers: Boolean = false, ignorePunctuation: Boolean = true, replaceUmlauts: Boolean = true) {

  implicit class StringCleanUtils(s: String) {
    private val removePunctuationPattern = """[\p{Punct}]""".r
    private val anyDigit = """\d""".r
    private val nonWordCharacters = """[\p{Cntrl}]|[^\p{ASCII}]""".r

    def removeNonWordCharacters(): String = nonWordCharacters.replaceAllIn(s, " ")

    def setCaseSensitivity(): String = if (ignoreLetterCase) s.toLowerCase(Locale.getDefault) else s

    def removePunctuation(): String = if (ignorePunctuation) removePunctuationPattern.replaceAllIn(s, " ") else s

    def removeNumbers(): String = if (ignoreNumbers) anyDigit.replaceAllIn(s, "") else s

    def transformUmlauts(): String = {
      if (replaceUmlauts) {
        s
          .replaceAll("ä", "ae")
          .replaceAll("ö", "oe")
          .replaceAll("ü", "ue")
          .replaceAll("ß", "ss")
          .replaceAll("æ", "ae")
          .replaceAll("œ", "oe")
          .replaceAll("Ä", "AE")
          .replaceAll("Ö", "OE")
          .replaceAll("Ü", "UE")
          .replaceAll("Æ", "AE")
          .replaceAll("Œ", "OE")
      } else s
    }
  }

  def stringCleanup(text: String): String = {
    text
      .setCaseSensitivity()
      .removePunctuation()
      .removeNumbers()
      .transformUmlauts()
      .removeNonWordCharacters()
  }

  def tokenize(text: String): List[String] = {
    val cleanString = stringCleanup(text)

    cleanString.split(" +").toList
  }
}