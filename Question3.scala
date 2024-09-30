import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import scala.jdk.CollectionConverters._  // Java to Scala collection converter

object TriadicClosureSparkHDFS {

  // Function to map phase: create pairs of friends for each user
  def mapPairsOfFriends(friendsMap: RDD[(Int, List[Int])]): RDD[(String, Int)] = {
    friendsMap.flatMap { case (userA, friends) =>
      for {
        i <- friends.indices
        j <- i + 1 until friends.length
      } yield {
        val friendB = friends(i)
        val friendC = friends(j)
        val pair = createSortedKey(friendB, friendC)
        (pair, userA) // Return the pair and the user as the mutual friend
      }
    }
  }

  // Function to reduce phase: check if triadic closure is satisfied
  def reduceAndCheckTriadicClosure(friendPairs: RDD[(String, Iterable[Int])], friendsMap: RDD[(Int, List[Int])]): RDD[String] = {
    // Collect the friend map to check direct connections locally
    val friendsMapCollected = friendsMap.collectAsMap().asScala.toMap

    friendPairs.flatMap { case (pair, mutualFriends) =>
      val friends = pair.split(",")
      val friendB = friends(0).toInt
      val friendC = friends(1).toInt

      // Check if B and C are directly connected
      if (!areDirectFriends(friendB, friendC, friendsMapCollected)) {
        List(s"Triadic closure not satisfied for pair ($friendB, $friendC) with mutual friends: ${mutualFriends.mkString(", ")}")
      } else {
        List.empty[String]  // No output if closure is satisfied
      }
    }
  }

  // Helper function to check if two users are direct friends
  def areDirectFriends(userA: Int, userB: Int, friendsMap: Map[Int, List[Int]]): Boolean = {
    friendsMap.get(userA).exists(_.contains(userB))
  }

  // Helper function to create a sorted key for a pair of friends
  def createSortedKey(friendA: Int, friendB: Int): String = {
    if (friendA < friendB) s"$friendA,$friendB" else s"$friendB,$friendA"
  }

  // Function to read the friends data from HDFS and create an RDD
  def readFriendsMapFromFile(sc: SparkContext, hdfsPath: String): RDD[(Int, List[Int])] = {
    sc.textFile(hdfsPath)
      .map { line =>
        val parts = line.split("\t")
        val user = parts(0).trim.toInt
        val friends = parts(1).split(",").map(_.trim.toInt).toList
        (user, friends)
      }
  }

  def main(args: Array[String]): Unit = {

    // Define HDFS paths for input and output
    val inputHDFS = "hdfs://localhost:9000/user/hthtd/InputFolder/example.txt"  // HDFS input file path
    val outputHDFS = "hdfs://localhost:9000/user/hthtd/OutputFolder" // HDFS output folder path

    // Initialize Spark context
    val conf = new SparkConf().setAppName("TriadicClosure").setMaster("local[*]") // Use all cores locally
    val sc = new SparkContext(conf)

    try {
      // Step 1: Read friends data from HDFS as RDD
      val friendsMapRDD = readFriendsMapFromFile(sc, inputHDFS)

      // Step 2: Map Phase - Create pairs of friends and record mutual friends
      val friendPairsRDD = mapPairsOfFriends(friendsMapRDD)
        .groupByKey() // Group by pair to collect all mutual friends

      // Step 3: Reduce Phase - Check for triadic closure and save unsatisfied trios
      val unsatisfiedTrios = reduceAndCheckTriadicClosure(friendPairsRDD, friendsMapRDD)

      // Save the output back to HDFS
      unsatisfiedTrios.saveAsTextFile(outputHDFS)
      
      println(s"Job completed successfully. Results saved to $outputHDFS")

    } catch {
      case e: Exception =>
        println(s"Error during Spark execution: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      // Stop the Spark context
      sc.stop()
    }
  }
}
