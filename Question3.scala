import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer

object MutualFriendsApp {

  val inputHDFS = "hdfs://localhost:9000/user/hthtd/InputFolder/example.txt"
  val outputHDFS = "hdfs://localhost:9000/user/hthtd/OutputFolder"

  val conf = new SparkConf().setAppName("MutualFriendsMapReduce").setMaster("local[*]")
  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {

    // Start time in milliseconds
    val startTime = System.currentTimeMillis()

    // Load the input file from HDFS
    val loadfile: RDD[String] = sc.textFile(inputHDFS)

    // Parse input file and create an RDD of (user, Set[friends])
    val userFriendsRDD: RDD[(String, Set[String])] = loadfile.flatMap { line =>
      val parts = line.split("\t")
      if (parts.length == 2) {
        val user = parts(0).trim
        val friends = parts(1).split(",").map(_.trim).filter(_.nonEmpty).toSet
        if (user.nonEmpty && friends.nonEmpty) {
          Some((user, friends))
        } else {
          None
        }
      } else {
        None
      }
    }

    // Collect userFriendsRDD as a map and broadcast it
    val userFriendsMap = userFriendsRDD.collectAsMap().toMap
    val userFriendsBroadcast = sc.broadcast(userFriendsMap)

    // Function to generate sorted user pair keys
    def generateUserPairKey(userA: String, userB: String): String = {
      if (userA < userB) s"$userA,$userB" else s"$userB,$userA"
    }

    // Function to compute mutual friends between two users
    def computeMutualFriends(friendsOfA: Set[String], userB: String): Set[String] = {
      val userFriendsMapBroadcast = userFriendsBroadcast.value
      val friendsOfB = userFriendsMapBroadcast.getOrElse(userB, Set.empty)
      val mutualFriends = friendsOfA intersect friendsOfB
      mutualFriends - userB
    }

    // Generate user pairs and compute mutual friends for each pair
    val mutualFriendsPairsRDD: RDD[(String, Set[String])] = userFriendsRDD.flatMap { case (userA, friendsOfA) =>
      val pairs = ListBuffer[(String, Set[String])]()
      for (userB <- friendsOfA) {
        val userPairKey = generateUserPairKey(userA, userB)
        val mutualFriends = computeMutualFriends(friendsOfA, userB)
        if (mutualFriends.nonEmpty) {
          pairs += ((userPairKey, mutualFriends))
        }
      }
      pairs
    }

    // Aggregate mutual friends for each pair
    val aggregatedMutualFriends: RDD[(String, Set[String])] = mutualFriendsPairsRDD.reduceByKey(_ ++ _)

    // Count the mutual friends for each user
    val userMutualFriendsCountRDD: RDD[(String, Int)] = aggregatedMutualFriends.flatMap { case (pair, mutualFriends) =>
      val users = pair.split(",")
      val userA = users(0)
      val userB = users(1)
      val mutualFriendsCount = mutualFriends.size
      Seq((userA, mutualFriendsCount), (userB, mutualFriendsCount))
    }.reduceByKey(_ + _)

    // Get top 10 users with the highest mutual friends
    val topUsers: Array[(String, Int)] = userMutualFriendsCountRDD
      .sortBy(_._2, ascending = false)
      .take(10)

    // Print top 10 users to the console
    println("\nTop Users with the Highest Mutual Friends:")
    topUsers.zipWithIndex.foreach { case ((user, count), rank) =>
      println(s"Rank ${rank + 1}: User $user ($count mutual friends)")
    }

    // Save the top 10 users to HDFS as a single file
    val topUsersRDD: RDD[String] = sc.parallelize(topUsers.map { case (user, count) =>
      s"User $user: $count mutual friends"
    })
    topUsersRDD.coalesce(1).saveAsTextFile(outputHDFS)

    // End time in milliseconds
    val endTime = System.currentTimeMillis()
    
    // Calculate and print duration in milliseconds
    val duration = endTime - startTime
    println(s"Task completed in $duration milliseconds")

    // Stop the Spark context
    sc.stop()
  }
}
