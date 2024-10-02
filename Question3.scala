import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer

// Initialize Spark context
val conf = new SparkConf().setAppName("MutualFriendsMapReduce").setMaster("local[*]")
val sc = new SparkContext(conf)

println("Spark context initialized")

// Define input and output HDFS paths
val inputHDFS = "hdfs://localhost:9000/user/hthtd/InputFolder/example.txt"
val outputHDFS = "hdfs://localhost:9000/user/hthtd/OutputFolder"

println(s"Input path: $inputHDFS")
println(s"Output path: $outputHDFS")

// Step 1: Load the input file from HDFS
val loadfile: RDD[String] = sc.textFile(inputHDFS)
println("Loaded input file from HDFS")

// Step 2: Parse the input data into a (user -> friends) RDD
val userFriendsRDD: RDD[(String, Set[String])] = loadfile.map { line =>
  val parts = line.split("\t")
  val user = parts(0)
  val friends = parts(1).split(",").toSet
  (user, friends)
}
println("Parsed user friends data")

// Step 3: Collect the user-friends map and broadcast it
val userFriendsMap = userFriendsRDD.collectAsMap()
val userFriendsBroadcast = sc.broadcast(userFriendsMap)
println("Broadcasted user friends map")

// Step 4: Split into multiple parts for clarity

// Part 4.1: Define a helper function to generate the user pair key
def generateUserPairKey(userA: String, userB: String): String = {
  if (userA < userB) s"$userA,$userB" else s"$userB,$userA"
}

// Part 4.2: Define a helper function to compute mutual friends for a pair of users
def computeMutualFriends(friendsOfA: Set[String], userB: String, userFriendsMap: Map[String, Set[String]]): Set[String] = {
  val friendsOfB = userFriendsMap.getOrElse(userB, Set.empty) // Get friends of userB
  val mutualFriends = friendsOfA intersect friendsOfB // Find mutual friends
  mutualFriends - userB // Remove userB from the mutual friends
}

// Part 4.3: Iterate over each user and their friends, generate pairs, and compute mutual friends
val mutualFriendsPairsRDD: RDD[(String, Set[String])] = userFriendsRDD.flatMap { case (userA, friendsOfA) =>
  val pairs = ListBuffer[(String, Set[String])]()
  val userFriendsMapBroadcast = userFriendsBroadcast.value // Access the broadcast variable

  // Iterate over friendsOfA to form pairs and compute mutual friends
  for (userB <- friendsOfA) {
    val userPairKey = generateUserPairKey(userA, userB) // Generate the user pair key
    val mutualFriends = computeMutualFriends(friendsOfA, userB, userFriendsMapBroadcast) // Compute mutual friends
    if (mutualFriends.nonEmpty) {
      pairs += ((userPairKey, mutualFriends)) // Add the result to the pairs buffer
    }
  }
  pairs
}
println("Computed mutual friends for user pairs")

// Step 5: Aggregate mutual friends for each user pair
val aggregatedMutualFriends: RDD[(String, Set[String])] = mutualFriendsPairsRDD.reduceByKey(_ ++ _)
println("Aggregated mutual friends for each user pair")

// Step 6: Output the mutual friends result
println("Map Output (Mutual Friends):")
aggregatedMutualFriends.collect().foreach { case (pair, mutualFriends) =>
  println(s"$pair -> ${mutualFriends.mkString(", ")}")
}

// Step 7: Compute the top users based on mutual friend count
val userMutualFriendsCountRDD: RDD[(String, Int)] = aggregatedMutualFriends.flatMap { case (pair, mutualFriends) =>
  val users = pair.split(",")
  val userA = users(0)
  val userB = users(1)
  val mutualFriendsCount = mutualFriends.size
  Seq((userA, mutualFriendsCount), (userB, mutualFriendsCount))
}
  .reduceByKey(_ + _)
println("Computed mutual friends count for each user")

// Step 8: Sort the users by mutual friends count in descending order and get the top 10
val topUsers: Array[(String, Int)] = userMutualFriendsCountRDD
  .sortBy(_._2, ascending = false)
  .take(10)

println("\nTop Users with the Highest Mutual Friends:")
topUsers.zipWithIndex.foreach { case ((user, count), rank) =>
  println(s"Rank ${rank + 1}: User $user ($count mutual friends)")
}

// Step 9: Save the results to HDFS
aggregatedMutualFriends.saveAsTextFile(outputHDFS)
println(s"Results saved to $outputHDFS")

// Step 10: Stop the Spark context
sc.stop()
println("Spark context stopped")
