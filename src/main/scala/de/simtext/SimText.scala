package main.scala.de.simtext

import java.io.File
import java.nio.file.{Paths, Files}

object SimText extends App {

  def printUsage() = println("Usage: run [Folder2/File1] [Folder2/File2] [outputfile]")
  def printNoFiles() = println(s"The arguments(${args.toList}) entered are no valid files.")

  if (args.length < 2) printUsage()
  else {
    if(args.forall(str => Files.exists(Paths.get(str)))) {
      val files: List[File] = args.toList.flatMap { fileString =>
        val path = Paths.get(fileString)
        if (Files.isDirectory(path)) {
          path.toFile.listFiles().toList
        } else {
          List(path.toFile)
        }
      }

      println(files)
    } else {
      printNoFiles()
    }
  }
}