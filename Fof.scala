import org.apache.spark.{SparkConf, SparkContext}

object FOFApp {

  def main(args: Array[String]): Unit = {

    // Initialize Spark Context
    val conf = new SparkConf().setAppName("FriendsRecommendation").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // Record start time in milliseconds
    val startTime = System.currentTimeMillis()

    val inputHDFS = "hdfs://localhost:9000/user/hthtd/InputFolder/soc-LiveJournal1Adj.txt"
    val outputHDFS = "hdfs://localhost:9000/user/hthtd/OutputFolder"
    val pairOutputHDFS = "hdfs://localhost:9000/user/hthtd/OutputFolder_Part3"

    //  Read input data from HDFS
    val loadfile = sc.textFile(inputHDFS)

    //  Parse input into user-friends relationships
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

    // Map Phase - Find friends of friends for all users
    val friendsOfFriendsRDD = sc.parallelize(friendsMapRDD.toSeq).flatMap { case (person, friends) =>
      val friendsOfFriends = friends.flatMap { friend =>
        friendsMapRDD.get(friend) match {
          case Some(friendsOfFriend) =>
            friendsOfFriend.filter(foaf => foaf != person && !friends.contains(foaf))
          case None => List.empty[Int]
        }
      }.toSet
      Some((person, friendsOfFriends))
    }

    //  Reduce Phase - Filter out direct friends and prepare the output format for all users
    val outputRDD = friendsOfFriendsRDD.map { case (person, potentialFriends) =>
      val directFriends = friendsMapRDD.getOrElse(person, List())
      val filteredFriends = potentialFriends -- directFriends
      (person, filteredFriends)
    }

    //  Save the potential friends for all users to HDFS
    val potentialFriendsRDD = outputRDD.map { case (person, potentialFriends) =>
      val potentialFriendsList = potentialFriends.toList.sorted.mkString(",")
      s"$person\t$potentialFriendsList"
    }
    potentialFriendsRDD.coalesce(1).saveAsTextFile(outputHDFS) // Save all potential friends to HDFS

    // Get top 10 users with the highest number of potential friends
    val topUsersRDD = outputRDD
      .map { case (person, potentialFriends) => (person, potentialFriends.size) }
      .sortBy(_._2, ascending = false)
      .take(10)  // Get top 10 users based on the size of their potential friends

    //  Convert the top 10 users list into an RDD for further processing
    val topUsersForProcessing = sc.parallelize(topUsersRDD)

    // Find shared friends of friends who are not direct friends between pairs of top 10 users
    val pairRDD = topUsersForProcessing.cartesian(topUsersForProcessing)
      .filter { case ((personA, _), (personB, _)) => personA < personB } // Avoid duplicate pairs
      .map { case ((personA, _), (personB, _)) =>
        val potentialFriendsA = outputRDD.lookup(personA).head
        val potentialFriendsB = outputRDD.lookup(personB).head
        val sharedFoaf = potentialFriendsA.intersect(potentialFriendsB) // Find the shared "friends of friends"
        ((personA, personB), sharedFoaf.size)
      }
      .filter { case (_, sharedCount) => sharedCount > 10 && sharedCount < 100 } // Apply the filter

    // Format the output for shared friends of friends between top 10 users
    val formattedPairRDD = pairRDD.map { case ((personA, personB), sharedCount) =>
      s"($personA, $personB) -> $sharedCount shared friends of friends"
    }


    // Save the filtered pairs to HDFS
    formattedPairRDD.coalesce(1).saveAsTextFile(pairOutputHDFS)

    // Record end time in milliseconds
    val endTime = System.currentTimeMillis()

    // Calculate and print the duration
    val duration = endTime - startTime
    println(s"Task completed in $duration milliseconds")

    // Stop the Spark Context
    sc.stop()
  }
}
