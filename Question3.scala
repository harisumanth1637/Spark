import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import scala.collection.mutable
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

// Step 3: Create user pairs and compute mutual friends
val mutualFriendsPairsRDD: RDD[(String, Set[String])] = userFriendsRDD.flatMap { case (userA, friendsOfA) =>
  val pairs = ListBuffer[(String, Set[String])]()
  
  // Create user pairs and calculate mutual friends
  for (userB <- friendsOfA) {
    // Ensure A < B for consistent ordering of user pairs
    val userPairKey = if (userA < userB) s"$userA,$userB" else s"$userB,$userA"
    val mutualFriends = friendsOfA intersect userFriendsRDD.lookup(userB).headOption.getOrElse(Set.empty)
    
    // Remove self from mutual friends
    val cleanedMutualFriends = mutualFriends - userA - userB

    if (cleanedMutualFriends.nonEmpty) {
      pairs += ((userPairKey, cleanedMutualFriends))
    }
  }
  pairs
}
println("Computed mutual friends for user pairs")

// Step 4: Aggregate mutual friends for each user pair
val aggregatedMutualFriends: RDD[(String, Set[String])] = mutualFriendsPairsRDD.reduceByKey(_ ++ _)
println("Aggregated mutual friends for each user pair")

// Step 5: Output the mutual friends result
println("Map Output (Mutual Friends):")
aggregatedMutualFriends.collect().foreach { case (pair, mutualFriends) =>
  println(s"$pair -> ${mutualFriends.mkString(", ")}")
}

// Step 6: Compute the top users based on mutual friend count
val userMutualFriendsCountRDD: RDD[(String, Int)] = aggregatedMutualFriends.flatMap { case (pair, mutualFriends) =>
  val users = pair.split(",")
  val userA = users(0)
  val userB = users(1)
  val mutualFriendsCount = mutualFriends.size
  Seq((userA, mutualFriendsCount), (userB, mutualFriendsCount))
}
  .reduceByKey(_ + _)
println("Computed mutual friends count for each user")

// Step 7: Sort the users by mutual friends count in descending order and get the top 10
val topUsers: Array[(String, Int)] = userMutualFriendsCountRDD
  .sortBy(_._2, ascending = false)
  .take(10)

println("\nTop Users with the Highest Mutual Friends:")
topUsers.zipWithIndex.foreach { case ((user, count), rank) =>
  println(s"Rank ${rank + 1}: User $user ($count mutual friends)")
}

// Step 8: Save the results to HDFS
aggregatedMutualFriends.saveAsTextFile(outputHDFS)
println(s"Results saved to $outputHDFS")

// Step 9: Stop the Spark context
sc.stop()
println("Spark context stopped")
