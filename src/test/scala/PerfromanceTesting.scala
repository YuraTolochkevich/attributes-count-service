import scala.collection.mutable.{ArrayBuffer, ListBuffer}

import org.scalatest.{FlatSpec, Matchers}

import scala.util.Random
class BenchMarkTest extends FlatSpec with Matchers {

  val allAttrinudesList = List("isMale",
    "isFemale", "isMerried", "Single", "Have_a_car", "Have_a_job","City_NY", "City_LA",
    "City_SF", "Is_superman", "Is_solder","Have_house")

  "Benchmark for 1000000 users vector" should "single and parallel solution " in {
    val usersVertorSize = 1000000
    val usersVector = FakeAttributesGenerator[String](usersVertorSize, allAttrinudesList).generateFakeAttributesVector

    measureTime[String](SingleThreadCountProcessor(), usersVector, Set("isMale", "Single", "Have_a_car"))
    measureTime[String](ParallelCountProcessor(8), usersVector, Set("isMale", "Single", "Have_a_car"))
  }

  "Benchmark for 2000000 users vector" should "single and parallel solution " in {
    val usersVertorSize = 2000000
    val usersVector = FakeAttributesGenerator[String](usersVertorSize, allAttrinudesList).generateFakeAttributesVector

    measureTime[String](SingleThreadCountProcessor(), usersVector, Set("isMale", "Single", "Have_a_car"))
    measureTime[String](ParallelCountProcessor(8), usersVector, Set("isMale", "Single", "Have_a_car"))
  }

  def measureTime[T](countProcessor: CountProcessor[T], usersAttrsVector: Vector[(Int, Set[T])], attribues: Set[T]) = {
    val start = System.currentTimeMillis()
    val resCount = countProcessor.queryCountForAttributes(usersAttrsVector, attribues)
    val finish = System.currentTimeMillis()
    println("Total time for tests for " + countProcessor.toString + ": "+
      (finish - start) + "ms. "+" resultCount: "+ resCount)
  }

}

case class FakeAttributesGenerator[T](attrVectorSize: Int, attributes: List[T]) {
  def generateFakeAttributesVector: Vector[(Int, Set[T])] = {
    val randomLengthGenerator = new Random()
    val randomUserIdGenerator = new Random()
    val listBuffer = ArrayBuffer[(Int, Set[T])]()

    val list  = ListBuffer[Set[T]]()
    for(i<- 0 to attributes.length*10){
      list+=Random.shuffle(attributes).take(randomLengthGenerator.nextInt(attributes.length - 1)).toSet
    }

    for (i <- 0 until attrVectorSize) {
      val currAttrSize = randomLengthGenerator.nextInt(attributes.length - 1)
      val attributesForUser = ArrayBuffer[T]()

      if (currAttrSize == 0) {
        listBuffer += ((randomUserIdGenerator.nextInt(), Set[T]()))
      } else {

        listBuffer += ((randomUserIdGenerator.nextInt(),
          Random.shuffle(attributes).take(randomLengthGenerator.nextInt(attributes.length - 1)).toSet))
      }
    }
    listBuffer.toVector
  }
}
