import org.apache.spark.{SparkConf, SparkContext}

object FofApp {

  def main(args: Array[String]): Unit = {

    // Initialize Spark Context
    val conf = new SparkConf().setAppName("FriendsRecommendation").setMaster("local[*]")
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

    // Step 3: Map Phase - Find friends of friends
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

    // Step 4: Reduce Phase - Filter out direct friends and prepare the output format
    val outputRDD = friendsOfFriendsRDD.map { case (person, potentialFriends) =>
      val directFriends = friendsMapRDD.getOrElse(person, List())
      val filteredFriends = potentialFriends -- directFriends
      val potentialFriendsList = filteredFriends.toList.sorted.mkString(",")
      s"$person\t$potentialFriendsList"
    }

    // Step 5: Print the output to the console
    outputRDD.foreach(println)

    // Step 6: Save the output to HDFS
    outputRDD.saveAsTextFile(outputHDFS)

    // -------- Additional functionality to find pairs of users sharing mutual friends of friends --------

    // Step 7: Find pairs of users sharing friends of friends
    val pairsRDD = friendsOfFriendsRDD.cartesian(friendsOfFriendsRDD)
      .filter { case ((personA, foafA), (personB, foafB)) =>
        personA < personB // Ensure uniqueness of pairs
      }
      .map { case ((personA, foafA), (personB, foafB)) =>
        val sharedFoaf = foafA intersect foafB
        ((personA, personB), sharedFoaf.size)
      }
      .filter { case (_, sharedCount) =>
        sharedCount > 10 && sharedCount < 100 // Filter by shared friends count
      }

    // Step 8: Format pairs output
    val pairsOutputRDD = pairsRDD.map { case ((personA, personB), sharedCount) =>
      s"($personA, $personB) -> $sharedCount shared friends of friends"
    }

    // Step 9: Print pairs output to console
    pairsOutputRDD.foreach(println)

    // Step 10: Save the pairs output to HDFS
    pairsOutputRDD.saveAsTextFile(part3OutputHDFS)

    // Record end time in milliseconds
    val endTime = System.currentTimeMillis()

    // Calculate and print the duration
    val duration = endTime - startTime
    println(s"Task completed in $duration milliseconds")

    // Stop the Spark Context
    sc.stop()
  }
}
