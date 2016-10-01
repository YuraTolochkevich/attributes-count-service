import scala.collection.immutable.IndexedSeq
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
/**
  * Created by itolochkevych on 01.10.16.
  */
trait CountProcessor[T] {
  import scala.concurrent.ExecutionContext.Implicits.global

  def getCountFutures(parallelFactor: Int, cache: Vector[(Int, Set[T])], attributes: Set[T])
                     (implicit validator: (Set[T], Set[T]) => Boolean): IndexedSeq[Future[Int]] = {

    def getSliceCountFuture(slicedCache: Vector[(Int, Set[T])]) = Future[Int] {
      val count = slicedCache.foldLeft(0) {
        (x: Int, y: (Int, Set[T])) => if (validator(y._2, attributes)) x + 1 else x
      }
      count
    }
    parallelFactor match {
      case 1 => IndexedSeq[Future[Int]](getSliceCountFuture(cache))
      case n => {
        val sliceSize = cache.length / (parallelFactor - 1)
        if (sliceSize == 0) List[Future[Int]](getSliceCountFuture(cache))
        for (i <- 0 until parallelFactor) yield getSliceCountFuture(cache.slice(i * sliceSize, (i + 1) * sliceSize))
      }
    }
  }

  def calcCountInParallel(parallelFactor: Int, cache: Vector[(Int, Set[T])], attributes: Set[T])
                        (implicit validator: (Set[T], Set[T]) => Boolean): Int = {
    val futures = getCountFutures(parallelFactor: Int, cache: Vector[(Int, Set[T])], attributes: Set[T])
    val futureSeq = Future.sequence(futures)
    val res = Await.result(futureSeq, 120.seconds)
    res.sum
  }
  def queryCountForAttributes(cache: Vector[(Int, Set[T])], attributes: Set[T]): Int
}

case class SingleThreadCountProcessor() extends CountProcessor[String] {
  import MatchAllValidator._
  override def queryCountForAttributes(cache: Vector[(Int, Set[String])], attributes: Set[String]): Int =
    calcCountInParallel(1,cache, attributes )
}
case class ParallelCountProcessor(parallelFactor: Int) extends CountProcessor[String] {
  require(parallelFactor>1)
  import MatchAllValidator._
  override def queryCountForAttributes(cache: Vector[(Int, Set[String])], attributes: Set[String]): Int =
    calcCountInParallel(parallelFactor,cache, attributes )
}



object MatchAllValidator {
  implicit val validator : (Set[String], Set[String]) =>Boolean = (x, y)=> x subsetOf y
  implicit val intValidator : (Set[Int], Set[Int]) =>Boolean = (x, y)=> x subsetOf y
}
