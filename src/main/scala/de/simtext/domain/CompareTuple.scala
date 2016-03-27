package de.simtext.domain

case class CompareTuple(tokenizedText1: TokenizedText, tokenizedText2: TokenizedText){
  val combinedTokens = tokenizedText1.tokens ++ tokenizedText2.tokens
  val splitIndex = tokenizedText1.tokens.length
}
