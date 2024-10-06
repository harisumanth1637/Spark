import org.apache.spark.{SparkConf, SparkContext}

object MutualFriendsFilterApp {

  def main(args: Array[String]): Unit = {

    // Initialize Spark Context
    val conf = new SparkConf().setAppName("MutualFriendsFilter").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // Hardcoded input and output paths
    val inputHDFS = "hdfs://localhost:9000/user/hthtd/InputFolder/soc-LiveJournal1Adj.txt"
    val outputHDFS = "hdfs://localhost:9000/user/hthtd/OutputFolder_Part3/"

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

    // Step 5: Find pairs (B, C) with more than 50 mutual friends
    val filteredPairsRDD = groupedPairsRDD.filter { case (_, mutualFriends) =>
      mutualFriends.size > 50
    }.map { case (pair, mutualFriends) =>
      s"($pair) -> ${mutualFriends.size} mutual friends"
    }

    // Step 6: Print the filtered results to the console
    filteredPairsRDD.collect().foreach(println)

    // Step 7: Save the output to HDFS
    filteredPairsRDD.coalesce(1).saveAsTextFile(outputHDFS)

    // Stop the Spark Context
    sc.stop()
  }
}
