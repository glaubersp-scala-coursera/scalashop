package scalashop

import java.util.concurrent.ForkJoinTask

import org.scalameter._
import common._

import scala.collection.mutable.ListBuffer

object HorizontalBoxBlurRunner {

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
      HorizontalBoxBlur.blur(src, dst, 0, height, radius)
    }
    println(s"sequential blur time: $seqtime ms")

    val numTasks = 32
    val partime = standardConfig measure {
      HorizontalBoxBlur.parBlur(src, dst, numTasks, radius)
    }
    println(s"fork/join blur time: $partime ms")
    println(s"speedup: ${seqtime / partime}")
  }
}

/** A simple, trivially parallelizable computation. */
object HorizontalBoxBlur {

  /** Blurs the rows of the source image `src` into the destination image `dst`,
    *  starting with `from` and ending with `end` (non-inclusive).
    *
    *  Within each row, `blur` traverses the pixels by going from left to right.
    */
  def blur(src: Img, dst: Img, from: Int, end: Int, radius: Int): Unit = {
    var yIndex = from
    while (yIndex < end) {
      var xIndex = 0
      while (xIndex < src.width) {
        dst.update(xIndex, yIndex, boxBlurKernel(src, xIndex, yIndex, radius))
        xIndex += 1
      }
      yIndex += 1
    }
  }

  /** Blurs the rows of the source image in parallel using `numTasks` tasks.
    *
    *  Parallelization is done by stripping the source image `src` into
    *  `numTasks` separate strips, where each strip is composed of some number of
    *  rows.
    */
  def parBlur(src: Img, dst: Img, numTasks: Int, radius: Int): Unit = {
    val rowsPerStrip = src.height / numTasks
    val rowsLastStrip = src.height % numTasks
    val hasRowsLastStrip = rowsLastStrip != 0
    val taskIndexEnd = if (hasRowsLastStrip) { numTasks - 1 } else { numTasks }

    val taskList = ListBuffer.empty[ForkJoinTask[Unit]]
    var taskIndex = 0
    while (taskIndex < taskIndexEnd) {
      val from = taskIndex * rowsPerStrip
      val end = (taskIndex + 1) * rowsPerStrip
      taskList += task {
        blur(src, dst, from, end, radius)
      }
      taskIndex += 1
    }
    if (hasRowsLastStrip) {
      val from = taskIndex * rowsPerStrip
      val end = from + rowsLastStrip
      taskList += task {
        blur(src, dst, from, end, radius)
      }
      taskIndex += 1
    }

    for (task <- taskList.toList) task.join
  }

}
