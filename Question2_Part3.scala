import org.apache.spark.{SparkConf, SparkContext}

object FOFApp {

  def main(args: Array[String]): Unit = {

    // Initialize Spark Context
    val conf = new SparkConf().setAppName("FriendsRecommendation").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // Record start time in milliseconds
    val startTime = System.currentTimeMillis()

    // Hardcoded input and output paths
    val inputHDFS = "hdfs://localhost:9000/user/hthtd/InputFolder/soc-LiveJournal1Adj.txt"
    val outputHDFS = "hdfs://localhost:9000/user/hthtd/OutputFolder"

    // Step 1: Read input data from HDFS
    val loadfile = sc.textFile(inputHDFS)

    // Step 2: Parse input into user-friends relationships
    val friendsMapRDD = loadfile.flatMap { line =>
      val parts = line.split("\t")
      if (parts.length == 2) {
        val user = parts(0).trim.toInt
        val friends = parts(1).split(",").map(_.trim.toInt).toList
        Some((user, friends))
      } else {
        None
      }
    }.collectAsMap()

    // Broadcast the friendsMapRDD for efficient lookup
    val broadcastFriendsMap = sc.broadcast(friendsMapRDD)

    // Step 3: Map Phase - Find friends of friends
    val friendsOfFriendsRDD = sc.parallelize(friendsMapRDD.toSeq).flatMap { case (person, friends) =>
      val friendsOfFriends = friends.flatMap { friend =>
        broadcastFriendsMap.value.get(friend) match {
          case Some(friendsOfFriend) =>
            friendsOfFriend.filter(foaf => foaf != person && !friends.contains(foaf))
          case None => List.empty[Int]
        }
      }.toSet
      Some((person, friendsOfFriends))
    }

    // Step 4: Reduce Phase - Filter out direct friends and prepare the output format
    val outputRDD = friendsOfFriendsRDD.map { case (person, potentialFriends) =>
      val directFriends = broadcastFriendsMap.value.getOrElse(person, List())
      val filteredFriends = potentialFriends -- directFriends
      (person, filteredFriends)
    }

    // Step 5: Get the top 10 users with the highest number of potential friends
    val top10Users = outputRDD
      .map { case (person, potentialFriends) => (person, potentialFriends.size) }
      .sortBy(_._2, ascending = false)
      .take(10)

    // Print the top 10 users with the most potential friends
    println("Top 10 users with the most potential friends:")
    top10Users.foreach { case (user, friendCount) =>
      println(s"User: $user, Potential Friends: $friendCount")
    }

    // Record end time in milliseconds
    val endTime = System.currentTimeMillis()

    // Calculate and print the duration
    val duration = endTime - startTime
    println(s"Task completed in $duration milliseconds")

    // Stop the Spark Context
    sc.stop()
  }
}
