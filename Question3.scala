import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

// Initialize Spark context
val conf = new SparkConf().setAppName("TriadicClosure").setMaster("local[*]")
val sc = new SparkContext(conf)

val inputHDFS = "hdfs://localhost:9000/user/hthtd/InputFolder/example.txt"  // HDFS input file path
val outputHDFS = "hdfs://localhost:9000/user/hthtd/OutputFolder"  // HDFS output folder path

// Step 1: Read friends data from HDFS as RDD
val friendsMapRDD: RDD[(Int, List[Int])] = sc.textFile(inputHDFS)
  .map { line =>
    val parts = line.split("\t")
    val user = parts(0).trim.toInt
    val friends = parts(1).split(",").map(_.trim.toInt).toList
    (user, friends)
  }

// Step 2: Create pairs of friends for each user and record mutual friends
val friendPairsRDD: RDD[(String, Int)] = friendsMapRDD.flatMap { case (userA, friends) =>
  for {
    i <- friends.indices
    j <- i + 1 until friends.length
  } yield {
    val friendB = friends(i)
    val friendC = friends(j)
    val pair = if (friendB < friendC) s"$friendB,$friendC" else s"$friendC,$friendB"
    (pair, userA) // Return the pair and the user as the mutual friend
  }
}.groupByKey() // Group by pair to collect all mutual friends

// Step 3: Create a direct friendships RDD for later checking
val directFriendships: RDD[(Int, Int)] = friendsMapRDD.flatMap { case (user, friends) =>
  friends.map(friend => (user, friend))
}

// Step 4: Check if B and C are directly connected and if triadic closure is satisfied
val unsatisfiedTrios: RDD[String] = friendPairsRDD.flatMap { case (pair, mutualFriends) =>
  val friends = pair.split(",")
  val friendB = friends(0).toInt
  val friendC = friends(1).toInt

  // Check if B and C are directly connected using the directFriendships RDD
  val isDirectlyConnected = directFriendships.filter {
    case (userA, userB) => (userA == friendB && userB == friendC) || (userA == friendC && userB == friendB)
  }.isEmpty() // True if there is no direct connection

  if (isDirectlyConnected) {
    Some(s"Triadic closure not satisfied for pair ($friendB, $friendC) with mutual friends: ${mutualFriends.mkString(", ")}")
  } else {
    None  // No output if closure is satisfied
  }
}

// Step 5: Collect and print the final results before saving to HDFS
val unsatisfiedTriosCollected = unsatisfiedTrios.collect()
println("Unsatisfied Triadic Closures:")
unsatisfiedTriosCollected.foreach(println)

// Step 6: Save the output back to HDFS
unsatisfiedTrios.saveAsTextFile(outputHDFS)

// Stop the Spark context
sc.stop()
