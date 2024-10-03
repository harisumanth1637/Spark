import org.apache.spark.{SparkConf, SparkContext}

object FofApp {

  def main(args: Array[String]): Unit = {

    // Initialize Spark Context
    val conf = new SparkConf().setAppName("FriendsRecommendation").setMaster("local[*]")
    
    // Set broadcast threshold to 100 MB
    conf.set("spark.sql.autoBroadcastJoinThreshold", (100 * 1024 * 1024).toString) // 100 MB threshold

    val sc = new SparkContext(conf)

    // Record start time in milliseconds
    val startTime = System.currentTimeMillis()

    // Hardcoded input and output paths
    val inputHDFS = "hdfs://localhost:9000/user/hthtd/InputFolder/example.txt"
    val outputHDFS = "hdfs://localhost:9000/user/hthtd/OutputFolder"
    val part3OutputHDFS = "hdfs://localhost:9000/user/hthtd/Part_3"

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

    // Broadcast the friendsMapRDD
    val broadcastFriendsMap = sc.broadcast(friendsMapRDD)

    // Step 3: Map Phase - Find friends of friends using the broadcasted friends map
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
      (person, filteredFriends)  // Return the user and their filtered friends of friends set
    }

    // Save the output to HDFS
    outputRDD.map { case (person, potentialFriends) =>
      val potentialFriendsList = potentialFriends.toList.sorted.mkString(",")
      s"$person\t$potentialFriendsList"
    }.coalesce(1).saveAsTextFile(outputHDFS)

    // Record end time in milliseconds
    val endTime = System.currentTimeMillis()
    val duration = endTime - startTime
    println(s"Task completed in $duration milliseconds")

    // Step 7: Broadcasting outputRDD in Step 7
    val broadcastOutputRDD = sc.broadcast(outputRDD.collectAsMap()) 

    // Find shared "friends of friends" who are not direct friends between two users
    val pairsRDD = outputRDD.flatMap { case (personA, foafA) =>
      broadcastOutputRDD.value.collect { case (personB, foafB) if personA < personB =>
        val sharedFoaf = foafA.intersect(foafB) // Find the shared "friends of friends"
        val directFriendsA = broadcastFriendsMap.value.getOrElse(personA, List()).toSet
        val directFriendsB = broadcastFriendsMap.value.getOrElse(personB, List()).toSet

        // Ensure the shared friends of friends are not direct friends for both users
        val validSharedFoaf = sharedFoaf.filter(foaf => !directFriendsA.contains(foaf) && !directFriendsB.contains(foaf))

        ((personA, personB), validSharedFoaf.size)
      }
    }
    .filter { case (_, sharedCount) =>
      sharedCount > 10 && sharedCount < 100 // Filter for valid pairs where the count is between 10 and 100
    }

    // Step 8: Format pairs output
    val pairsOutputRDD = pairsRDD.map { case ((personA, personB), sharedCount) =>
      s"($personA, $personB) -> $sharedCount shared friends of friends"
    }

    // Step 10: Save the pairs output to HDFS
    pairsOutputRDD.coalesce(1).saveAsTextFile(part3OutputHDFS)



    // Stop the Spark Context
    sc.stop()
  }
}
