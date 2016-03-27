package de.simtext.domain

case class CompareResult(name1: String, name2: String, similarity: Double) {
  override def toString() = s"$name1 to $name2 => $similarity"
}
