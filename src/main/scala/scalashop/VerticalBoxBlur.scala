package scalashop

import java.util.concurrent.ForkJoinTask

import org.scalameter._
import common._

import scala.collection.mutable.ListBuffer

object VerticalBoxBlurRunner {

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 5,
    Key.exec.maxWarmupRuns -> 10,
    Key.exec.benchRuns -> 10,
    Key.verbose -> true
  ) withWarmer (new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val radius = 3
    val width = 1920
    val height = 1080
    val src = new Img(width, height)
    val dst = new Img(width, height)
    val seqtime = standardConfig measure {
      VerticalBoxBlur.blur(src, dst, 0, width, radius)
    }
    println(s"sequential blur time: $seqtime ms")

    val numTasks = 32
    val partime = standardConfig measure {
      VerticalBoxBlur.parBlur(src, dst, numTasks, radius)
    }
    println(s"fork/join blur time: $partime ms")
    println(s"speedup: ${seqtime / partime}")
  }

}

/** A simple, trivially parallelizable computation. */
object VerticalBoxBlur {

  /** Blurs the columns of the source image `src` into the destination image
    *  `dst`, starting with `from` and ending with `end` (non-inclusive).
    *
    *  Within each column, `blur` traverses the pixels by going from top to
    *  bottom.
    */
  def blur(src: Img, dst: Img, from: Int, end: Int, radius: Int): Unit = {
    var xIndex = from
    while (xIndex < end) {
      var yIndex = 0
      while (yIndex < src.height) {
        dst.update(xIndex, yIndex, boxBlurKernel(src, xIndex, yIndex, radius))
        yIndex += 1
      }
      xIndex += 1
    }
  }

  /** Blurs the columns of the source image in parallel using `numTasks` tasks.
    *
    *  Parallelization is done by stripping the source image `src` into
    *  `numTasks` separate strips, where each strip is composed of some number of
    *  columns.
    */
  def parBlur(src: Img, dst: Img, numTasks: Int, radius: Int): Unit = {
    val colsPerStrip = src.width / numTasks
    val colsLastStrip = src.width % numTasks
    val hasColsLastStrip = colsLastStrip != 0
    val taskIndexEnd = if (hasColsLastStrip) { numTasks - 1 } else { numTasks }

    var taskList = ListBuffer.empty[ForkJoinTask[Unit]]
    var taskIndex = 0
    while (taskIndex < taskIndexEnd) {
      val from = taskIndex * colsPerStrip
      val end = (taskIndex + 1) * colsPerStrip
      taskList += task {
        blur(src, dst, from, end, radius)
      }
      taskIndex += 1
    }
    if (hasColsLastStrip) {
      val from = taskIndex * colsPerStrip
      val end = from + colsLastStrip
      taskList += task {
        blur(src, dst, from, end, radius)
      }
      taskIndex += 1
    }

    for (task <- taskList.toList) task.join
  }

}
