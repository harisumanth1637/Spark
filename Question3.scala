import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer

// Initialize Spark context
val conf = new SparkConf().setAppName("TriadicClosure").setMaster("local[*]")
val sc = new SparkContext(conf)

try {
  // Define input and output HDFS paths
  val inputHDFS = "hdfs://localhost:9000/user/hthtd/InputFolder/example.txt"
  val outputHDFS = "hdfs://localhost:9000/user/hthtd/OutputFolder"

  // Step 1: Load the input file from HDFS
  val loadfile: RDD[String] = sc.textFile(inputHDFS)

  // Step 2: Split the lines into user and friends (Assume the file contains lines like: "1 2,3,4")
  val splitLines: RDD[Array[String]] = loadfile.map(line => line.split("\t"))

  // Step 3: Map each line to a (user, List(friends)) pair
  val friendsMapRDD: RDD[(Int, List[Int])] = splitLines.map { parts =>
    val user = parts(0).trim.toInt
    val friends = parts(1).split(",").map(_.trim.toInt).toList
    (user, friends)
  }

  // Step 4: Broadcast the direct friendships map
  val directFriendshipsMap = friendsMapRDD.collectAsMap()
  val directFriendshipsBroadcast = sc.broadcast(directFriendshipsMap)

  // Step 5: Create pairs of friends for each user
  val friendPairsRDD: RDD[(String, Int)] = friendsMapRDD.flatMap { case (userA, friends) =>
    val pairs = ListBuffer[(String, Int)]()
    for {
      i <- friends.indices
      j <- i + 1 until friends.length
    } {
      val friendB = friends(i)
      val friendC = friends(j)
      val pair = if (friendB < friendC) s"$friendB,$friendC" else s"$friendC,$friendB"
      pairs += ((pair, userA)) // Return the pair and the user as the mutual friend
    }
    pairs
  }

  // Step 6: Group the pairs by key to collect all mutual friends for each pair
  val groupedFriendPairs: RDD[(String, Iterable[Int])] = friendPairsRDD.groupByKey()

  // Step 7: Check if the pairs are directly connected or not (triadic closure check)
  val unsatisfiedTrios: RDD[String] = groupedFriendPairs.flatMap { case (pair, mutualFriends) =>
    val friends = pair.split(",")
    val friendB = friends(0).toInt
    val friendC = friends(1).toInt

    // Use the broadcasted direct friendship map to check if they are directly connected
    val isDirectlyConnected = {
      directFriendshipsBroadcast.value.get(friendB) match {
        case Some(friendsList) => friendsList.contains(friendC)
        case None => false
      }
    } || {
      directFriendshipsBroadcast.value.get(friendC) match {
        case Some(friendsList) => friendsList.contains(friendB)
        case None => false
      }
    }

    if (!isDirectlyConnected) {
      Some(s"Triadic closure not satisfied for pair ($friendB, $friendC) with mutual friends: ${mutualFriends.mkString(", ")}")
    } else {
      None
    }
  }

  // Step 8: Collect the final results (with Spark context still active)
  val unsatisfiedTriosCollected: Array[String] = unsatisfiedTrios.collect()

  // Step 9: Print the unsatisfied triadic closures
  unsatisfiedTriosCollected.foreach(println)

  // Step 10: Save the results to HDFS
  unsatisfiedTrios.saveAsTextFile(outputHDFS)

} catch {
  case e: Exception =>
    println(s"Error occurred: ${e.getMessage}")
    e.printStackTrace()
} finally {
  // Step 11: Stop the Spark context at the end
  sc.stop()
}
