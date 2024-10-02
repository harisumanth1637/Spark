import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer

object MutualFriendsApp {

  val inputHDFS = "hdfs://localhost:9000/user/hthtd/InputFolder/example.txt"
  val outputHDFS = "hdfs://localhost:9000/user/hthtd/OutputFolder"

  val conf = new SparkConf().setAppName("MutualFriendsMapReduce").setMaster("local[*]")
  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {

    val startTime = System.nanoTime()

    val loadfile: RDD[String] = sc.textFile(inputHDFS)

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

    val userFriendsMap = userFriendsRDD.collectAsMap().toMap
    val userFriendsBroadcast = sc.broadcast(userFriendsMap)

    def generateUserPairKey(userA: String, userB: String): String = {
      if (userA < userB) s"$userA,$userB" else s"$userB,$userA"
    }

    def computeMutualFriends(friendsOfA: Set[String], userB: String): Set[String] = {
      val userFriendsMapBroadcast = userFriendsBroadcast.value
      val friendsOfB = userFriendsMapBroadcast.getOrElse(userB, Set.empty)
      val mutualFriends = friendsOfA intersect friendsOfB
      mutualFriends - userB
    }

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

    val aggregatedMutualFriends: RDD[(String, Set[String])] = mutualFriendsPairsRDD.reduceByKey(_ ++ _)

    val userMutualFriendsCountRDD: RDD[(String, Int)] = aggregatedMutualFriends.flatMap { case (pair, mutualFriends) =>
      val users = pair.split(",")
      val userA = users(0)
      val userB = users(1)
      val mutualFriendsCount = mutualFriends.size
      Seq((userA, mutualFriendsCount), (userB, mutualFriendsCount))
    }.reduceByKey(_ + _)

    val topUsers: Array[(String, Int)] = userMutualFriendsCountRDD
      .sortBy(_._2, ascending = false)
      .take(10)

    // Print topUsers to console
    println("\nTop Users with the Highest Mutual Friends:")
    topUsers.zipWithIndex.foreach { case ((user, count), rank) =>
      println(s"Rank ${rank + 1}: User $user ($count mutual friends)")
    }

    // Save topUsers to outputHDFS
    val topUsersRDD: RDD[String] = sc.parallelize(topUsers.map { case (user, count) =>
      s"User $user: $count mutual friends"
    })

    topUsersRDD.coalesce(1).saveAsTextFile(outputHDFS)

    // End time measurement
    val endTime = System.nanoTime()
    val duration = (endTime - startTime) / 1e9d
    println(s"Task completed in $duration seconds")

    sc.stop()
  }
}
