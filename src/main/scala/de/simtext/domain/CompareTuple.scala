package de.simtext.domain

import de.simtext.Tokenizer


case class CompareTuple(private val file1: String, private val file2: String, tokenizer: Tokenizer) {
  val tokens1 = tokenizer.tokenize(file1)
  val tokens2 = tokenizer.tokenize(file2)
}
