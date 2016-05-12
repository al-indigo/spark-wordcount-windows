package ru.ispras

import org.apache.spark.rdd.RDD
import org.apache.spark.{Logging, SparkContext, SparkConf}
import org.apache.log4j.Logger
import org.apache.log4j.Level

import java.io.File
import org.apache.commons.io.FileUtils.cleanDirectory

trait AbstractFS {
  def getFullPath(path: String): String
  def listDir(path: String): Array[String]
  def cleanDir(path: String)
}

class LocalFS extends AbstractFS {
  def getFullPath(path: String) = "file:///" + path
  def listDir(path: String): Array[String] = {
    val d = new File(path)
    if (d.exists && d.isDirectory) {
    d.listFiles.filter(_.isFile).map(_.toString)
    } else {
      Array[String] ()
    }
  }
  def cleanDir(path: String) { cleanDirectory(new File(path)); }
}

class HadoopFS extends AbstractFS {
  def getFullPath(path: String) = "hdfs://" + path
  def listDir(path: String): Array[String] = new Array[String](0)
  def cleanDir(path: String) {}
}


object Main extends Logging {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    if (args.length < 3) {
      val className = this.getClass.getName.stripSuffix("$")
      println(
        s"""
          |usage (local run): mvn exec:java -Dexec.mainClass=$className -Dexec.args="<filesystem_type> <src_dir> <res_dir> <window_size>"
        """.stripMargin)
      System.exit(1)
    }

    val fsType = args(0)
    val src_dir = args(1)
    val res_dir = args(2)
    val window = args(3).toInt

    val localMaster = "local[1]"
    val conf = new SparkConf().setAppName("WordCountWindow")
    // check whether local run or not
    // spark-submit will automatically set spark.master in cluster run
    val master = conf.get("spark.master", localMaster)
    val localRun = master.startsWith("local")
    if (localRun)
      conf.setMaster(master)
    val sc = new SparkContext(conf)

    val fs: AbstractFS = fsType match {
      case "local" => new LocalFS
      case "hdfs" => new HadoopFS
      case _ => { println("Unsupported fs type, exiting"); System.exit(1); new LocalFS }
      // TODO: add swift and ceph
    }

    val wcw = new WordCountWindow(sc, src_dir, res_dir, window, fs)
    wcw.calculate()

    // can't do sc.stop() (or nothing) here since it causes program to exit with non-zero code
    // see http://stackoverflow.com/questions/28362341/error-utils-uncaught-exception-in-thread-sparklistenerbus
    if (localRun)
      System.exit(0)

  }

}

class WordCountWindow(val sc: SparkContext, val src_dir: String, val res_dir: String, val window: Int,
                      val fs: AbstractFS) extends Logging {
  def calculate(): Unit = {
    fs.cleanDir(res_dir) // we need to clean res dir since spark will not overwrite files
    val files = fs.listDir(src_dir)
//    files.foreach(println)
    logInfo(s"Number of files: " + files.size)
    for (window_start <- 0 to files.size - window) {
      val window_end = window_start + window
      logInfo(s"Calculating window ($window_start, $window_end)")
      val window_files = files slice (window_start, window_end)
      calcWindow(window_files, window_start.toString + "-" + window_end.toString)
    }
  }

  private def calcWindow(windowFiles: Array[String], windowName: String): Unit = {
    val zeroRDD: RDD[(String, Int)] = sc.emptyRDD
    val counts: RDD[(String, Int)] = windowFiles.foldLeft(zeroRDD)( (countsRDD, srcFile) => {
      val textFile = sc.textFile(fs.getFullPath(srcFile))
      val srcFileWords = textFile.flatMap(line => line.split(" "))
        .map(word => (word, 1))
      val result = srcFileWords.union(countsRDD).reduceByKey(_ + _)
      result
    })
    counts.saveAsTextFile(fs.getFullPath(res_dir + "/window_" + windowName))
//    counts.collect().foreach(println)
  }
}
