import org.apache.spark.{SparkConf, SparkContext}

object TriadicClosureApp {

  def main(args: Array[String]): Unit = {

    // Initialize Spark Context
    val conf = new SparkConf().setAppName("TriadicClosure").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // Record start time in milliseconds
    val startTime = System.currentTimeMillis()

    // Hardcoded input and output paths
    val inputHDFS = "hdfs://localhost:9000/user/hthtd/InputFolder/example.txt"
    val outputHDFS = "hdfs://localhost:9000/user/hthtd/OutputFolder"
    val highMutualFriendsOutputHDFS = "hdfs://localhost:9000/user/hthtd/HighMutualFriendsOutputFolder"

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

    // Broadcast the userFriendsRDD as a map so that it can be accessed in transformations
    val userFriendsMap = userFriendsRDD.collectAsMap()
    val broadcastFriendsMap = sc.broadcast(userFriendsMap)

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

    // Step 5: Filter pairs where mutual friends are greater than 50
    val highMutualFriendsRDD = groupedPairsRDD.filter { case (_, mutualFriends) =>
      mutualFriends.size > 50
    }.map { case (pair, mutualFriends) =>
      s"Pair $pair has ${mutualFriends.size} mutual friends"
    }

    // Step 6: Check if the triadic closure is satisfied
    val triadicClosureResults = groupedPairsRDD.flatMap { case (pair, mutualFriends) =>
      val friends = pair.split(",")
      val friendB = friends(0).toInt
      val friendC = friends(1).toInt

      // Access the broadcasted map
      val friendsMap = broadcastFriendsMap.value

      // Check if B and C are directly connected
      val friendsOfB = friendsMap.getOrElse(friendB, List())
      val isDirectlyConnected = friendsOfB.contains(friendC)

      if (!isDirectlyConnected) {
        Some(s"(${mutualFriends.mkString(", ")}, $friendB, $friendC) -> Triadic closure not satisfied ($friendB and $friendC are not connected)")
      } else {
        None
      }
    }

    // Step 7: Save the triadic closure results to HDFS
    sc.parallelize(triadicClosureResults.collect()).coalesce(1).saveAsTextFile(outputHDFS)

    // Step 8: Save pairs with more than 50 mutual friends to HDFS
    highMutualFriendsRDD.coalesce(1).saveAsTextFile(highMutualFriendsOutputHDFS)

    // Record end time in milliseconds
    val endTime = System.currentTimeMillis()

    // Calculate and print the duration
    val duration = endTime - startTime
    println(s"Task completed in $duration milliseconds")

    // Stop the Spark Context
    sc.stop()
  }
}
