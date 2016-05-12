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
          |usage (local run): mvn exec:java -Dexec.mainClass=$className -Dexec.args="<src_dir> <res_dir> <window_size>"
        """.stripMargin)
      System.exit(1)
    }

    val src_dir = args(0)
    val dest_file = args(1)
    val window = args(2).toInt

    val localMaster = "local[1]"
    val conf = new SparkConf().setAppName("WordCountWindow")
    // check whether local run or not
    // spark-submit will automatically set spark.master in cluster run
    // val master = conf.get("spark.master", localMaster)
    // val localRun = master.sta rtsWith("local")
//    if (localRun)
//      conf.setMaster(master)
    val sc = new SparkContext(conf)


    val wcw = new WordCountWindow(sc, src_dir, dest_file, window)
    wcw.calculate()

    // can't do sc.stop() (or nothing) here since it causes program to exit with non-zero code
    // see http://stackoverflow.com/questions/28362341/error-utils-uncaught-exception-in-thread-sparklistenerbus
//    if (localRun)
//      System.exit(0)

  }

}

class WordCountWindow(val sc: SparkContext, val src_dir: String, val dest_file: String, val window: Int) extends Logging {
  def calculate(): Unit = {
    println(sc.hadoopConfiguration.toString())
    val dirPath = new Path(src_dir)
    val files_statuses = FileSystem.get(new URI(src_dir), sc.hadoopConfiguration).listStatus(dirPath)
    val files = files_statuses.map(_.getPath).filter(_ != dirPath)
    val files_as_strings = files.map(_.toUri().toString())
    val files_number = files.size
    logInfo(s"Number of files: $files_number")
    logInfo("Found files: ")
    files_as_strings.foreach(println)
    val windows_number = files_number - window
    val pw = new PrintWriter(new File(dest_file))
    for (window_start <- 0 to windows_number) {
      val window_end = window_start + window
      logInfo("Calculated windows percentage: " + window_start * 1.0 / windows_number)
      logInfo(s"Calculating window ($window_start, $window_end)")
      val window_files = files_as_strings slice (window_start, window_end)
      calcWindow(window_files, window_start.toString + "-" + window_end.toString, pw)
    }
    pw.close()
  }

  private def calcWindow(windowFiles: Array[String], windowName: String, pw: PrintWriter): Unit = {
    val counts: Long = windowFiles.foldLeft(0L)( (currentCounts, srcFile) => {
      val textFile = sc.textFile(srcFile)
      val srcFileCounts = textFile.flatMap(line => line.split(" ")).count()
      currentCounts + srcFileCounts
    })
//    counts.saveAsTextFile(fs.getFullPath(res_dir + "/window_" + windowName))
    logInfo(s"WordCount for window $windowName is $counts")
    pw.write(windowName + " " + counts + "\n")
  }
}
