import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

// Initialize Spark context
val conf = new SparkConf().setAppName("TriadicClosure").setMaster("local[*]")
val sc = new SparkContext(conf)

// Define input and output HDFS paths
val inputHDFS = "hdfs://localhost:9000/user/gs37r/InputFolder/input1.txt"
val outputHDFS = "hdfs://localhost:9000/user/gs37r/OutputFolder"

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

// Step 4: Create pairs of friends for each user
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
}

// Step 5: Group the pairs by key to collect all mutual friends for each pair
val groupedFriendPairs: RDD[(String, Iterable[Int])] = friendPairsRDD.groupByKey()

// Step 6: Flatten the friends map to create direct friendship pairs
val directFriendships: RDD[(Int, Int)] = friendsMapRDD.flatMap { case (user, friends) =>
  friends.map(friend => (user, friend))
}

// Step 7: Check if the pairs are directly connected or not (triadic closure check)
val unsatisfiedTrios: RDD[String] = groupedFriendPairs.flatMap { case (pair, mutualFriends) =>
  val friends = pair.split(",")
  val friendB = friends(0).toInt
  val friendC = friends(1).toInt

  val isDirectlyConnected = directFriendships.filter {
    case (userA, userB) => (userA == friendB && userB == friendC) || (userA == friendC && userB == friendB)
  }.isEmpty()

  if (isDirectlyConnected) {
    Some(s"Triadic closure not satisfied for pair ($friendB, $friendC) with mutual friends: ${mutualFriends.mkString(", ")}")
  } else {
    None
  }
}

// Step 8: Collect the final results
val unsatisfiedTriosCollected: Array[String] = unsatisfiedTrios.collect()

// Step 9: Print the unsatisfied triadic closures
unsatisfiedTriosCollected.foreach(println)

// Step 10: Save the results to HDFS
unsatisfiedTrios.saveAsTextFile(outputHDFS)

// Step 11: Stop the Spark context
sc.stop()
