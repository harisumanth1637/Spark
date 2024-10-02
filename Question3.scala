import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer

// Initialize Spark context
val conf = new SparkConf().setAppName("TriadicClosure").setMaster("local[*]")
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

// Step 2: Split the lines into user and friends (Assume the file contains lines like: "1 2,3,4")
val splitLines: RDD[Array[String]] = loadfile.map(line => line.split("\t"))
println("Split the input lines")

// Step 3: Map each line to a (user, List(friends)) pair
val friendsMapRDD: RDD[(Int, List[Int])] = splitLines.map { parts =>
  val user = parts(0).trim.toInt
  val friends = parts(1).split(",").map(_.trim.toInt).toList
  (user, friends)
}
println("Mapped users to their friends list")

// Step 4: Create a direct friendship RDD where each pair of friends (user, friend) is represented as (userA, userB)
val directFriendshipsRDD: RDD[(Int, Int)] = friendsMapRDD.flatMap { case (user, friends) =>
  friends.map(friend => (user, friend)) // Create a (user, friend) pair for each friendship
}
println("Created direct friendship pairs")

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
println("Created friend pairs for each user")

// Step 6: Group the pairs by key to collect all mutual friends for each pair
val groupedFriendPairs: RDD[(String, Iterable[Int])] = friendPairsRDD.groupByKey()
println("Grouped friend pairs by key")

// Step 7: Use a self-join on the directFriendshipsRDD to check if the pair is directly connected
val unsatisfiedTrios: RDD[String] = groupedFriendPairs.flatMap { case (pair, mutualFriends) =>
  val friends = pair.split(",")
  val friendB = friends(0).toInt
  val friendC = friends(1).toInt

  // Check if there is a direct connection between friendB and friendC
  val directConnection = directFriendshipsRDD
    .filter { case (userA, userB) => (userA == friendB && userB == friendC) || (userA == friendC && userB == friendB) }
    .isEmpty()

  if (directConnection) {
    Some(s"Triadic closure not satisfied for pair ($friendB, $friendC) with mutual friends: ${mutualFriends.mkString(", ")}")
  } else {
    None
  }
}
println("Checked for triadic closure")

// Step 8: Collect the final results (with Spark context still active)
val unsatisfiedTriosCollected: Array[String] = unsatisfiedTrios.collect()
println("Collected unsatisfied triadic closures")

// Step 9: Print the unsatisfied triadic closures
unsatisfiedTriosCollected.foreach(println)

// Step 10: Save the results to HDFS
unsatisfiedTrios.saveAsTextFile(outputHDFS)
println(s"Saved the results to $outputHDFS")

// Step 11: Stop the Spark context at the end
sc.stop()
println("Spark context stopped")
