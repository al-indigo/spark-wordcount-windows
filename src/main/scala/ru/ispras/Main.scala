package ru.ispras

import java.net.URI
import java.io._
import org.apache.spark.rdd.RDD
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.hadoop.fs.{FileSystem, Path}

object Main extends Logging {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    if (args.length < 3) {
      val className = this.getClass.getName.stripSuffix("$")
      println(
        s"""
          |usage (local run): mvn exec:java -Dexec.mainClass=$className -Dexec.args="<src_dir> <dest_file> <window_size> [number_of_partitions]"
        """.stripMargin)
      System.exit(1)
    }

    val src_dir = args(0)
    val dest_file = args(1)
    val window = args(2).toInt
    val partitons_number: Option[Int] = if (args.length >= 4) Some(args(3).toInt) else None

    val conf = new SparkConf().setAppName("WordCountWindow")
    val sc = new SparkContext(conf)


    val wcw = new WordCountWindow(sc, src_dir, dest_file, window, partitons_number)
    wcw.calculate()


  }
}

class WordCountWindow(val sc: SparkContext, val src_dir: String, val dest_file: String, val window: Int, partitions_number: Option[Int]) extends Logging {
  def calculate(): Unit = {
    println(sc.hadoopConfiguration.toString())
    val dirPath = new Path(src_dir)
    val files_statuses = FileSystem.get(new URI(src_dir), sc.hadoopConfiguration).listStatus(dirPath)
    val files = files_statuses.map(_.getPath).filter(_ != dirPath)
    val files_as_strings = files.map(_.toUri().toString())
    val files_number = files.size
    logInfo(s"Number of files: $files_number")
    val windows_number = files_number - window
    val step = window/4
    val pw = new PrintWriter(new File(dest_file))
    try {
      for (window_start <- 0 to windows_number by step) {
        val window_end = window_start + window
        logInfo("Calculated windows percentage: " + window_start * step.toDouble / (windows_number + 1))
        logInfo(s"Calculating window ($window_start, $window_end)")
        val window_files = files_as_strings slice(window_start, window_end)
        calcWindow(window_files, window_start.toString + "-" + window_end.toString, pw)
      }
    }
    finally {
      pw.close()
    }
  }

  private def calcWindow(windowFiles: Array[String], windowName: String, pw: PrintWriter): Unit = {
    val text_files = partitions_number match {
      case Some(pn) => sc.textFile(windowFiles.mkString(","), pn)
      case None => sc.textFile(windowFiles.mkString(","))
    }
    val wcRDD = text_files.flatMap(line => line.split(" "))
    val real_partitions_number = wcRDD.partitions.size
    val counts = wcRDD.count()
    logInfo(s"Number of of partitions: $real_partitions_number")
    logInfo(s"WordCount for window $windowName is $counts")
    pw.write(windowName + " " + counts + "\n")
  }
}
