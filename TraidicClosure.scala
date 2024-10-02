import org.apache.spark.{SparkConf, SparkContext}

object TriadicClosureApp {

  def main(args: Array[String]): Unit = {

    // Initialize Spark Context
    val conf = new SparkConf().setAppName("TriadicClosure").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // Hardcoded input and output paths
    val inputHDFS = "hdfs://localhost:9000/user/hthtd/InputFolder/example.txt"
    val outputHDFS = "hdfs://localhost:9000/user/hthtd/OutputFolder"

    // Step 1: Read input data from HDFS
    val loadfile = sc.textFile(inputHDFS)

    // Step 2: Parse input into user-friends relationships
    val userFriendsRDD = loadfile.flatMap { line =>
      val parts = line.split("\t")
      if (parts.length == 2) {
        val user = parts(0).trim.toInt
        val friends = parts(1).split(",").map(_.trim.toInt).toList
        Some((user, friends))
      } else {
        None
      }
    }

    // Step 3: Generate pairs of friends (B, C) and associate the user (A) as the mutual friend
    val friendPairsRDD = userFriendsRDD.flatMap { case (userA, friends) =>
      for {
        i <- friends.indices
        j <- i + 1 until friends.length
      } yield {
        val friendB = friends(i)
        val friendC = friends(j)
        val pair = if (friendB < friendC) s"$friendB,$friendC" else s"$friendC,$friendB"
        (pair, userA)
      }
    }

    // Step 4: Group by pairs of friends to collect all mutual friends
    val groupedPairsRDD = friendPairsRDD.groupByKey()

    // Step 5: Check if the triadic closure is satisfied
    val triadicClosureResults = groupedPairsRDD.flatMap { case (pair, mutualFriends) =>
      val friends = pair.split(",")
      val friendB = friends(0).toInt
      val friendC = friends(1).toInt

      // Check if B and C are directly connected by looking into the global user-friends network
      val isDirectlyConnected = userFriendsRDD.filter { case (user, friendsOfUser) =>
        (user == friendB && friendsOfUser.contains(friendC)) || (user == friendC && friendsOfUser.contains(friendB))
      }.isEmpty()  // True if no direct connection exists between B and C

      if (isDirectlyConnected) {
        Some(s"($mutualFriends.mkString(", "), $friendB, $friendC) -> Triadic closure not satisfied ($friendB and $friendC are not connected)")
      } else {
        None
      }
    }

    // Step 6: Print the results to the console
    val results = triadicClosureResults.collect()
    results.foreach(println)

    // Step 7: Save the output to HDFS
    sc.parallelize(results).coalesce(1).saveAsTextFile(outputHDFS)

    // Stop the Spark Context
    sc.stop()
  }
}
