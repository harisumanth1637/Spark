import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object TriadicClosureApp {

  val inputHDFS = "hdfs://localhost:9000/user/hthtd/InputFolder/example.txt"
  val outputHDFS = "hdfs://localhost:9000/user/hthtd/OutputFolder"

  val conf = new SparkConf().setAppName("TriadicClosure").setMaster("local[*]")
  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {

    // Start time in milliseconds
    val startTime = System.currentTimeMillis()

    // Load the input file from HDFS
    val loadfile: RDD[String] = sc.textFile(inputHDFS)

    // Parse input and create an RDD of (user, friendsList)
    val userFriendsRDD: RDD[(Int, List[Int])] = loadfile.flatMap { line =>
      val parts = line.split("\t")
      if (parts.length == 2) {
        val user = parts(0).trim.toInt
        val friends = parts(1).split(",").map(_.trim.toInt).toList
        if (user > 0 && friends.nonEmpty) {
          Some((user, friends))
        } else {
          None
        }
      } else {
        None
      }
    }

    // Add a print to ensure the data is correctly parsed
    println("Parsed userFriendsRDD:")
    userFriendsRDD.take(10).foreach(println)

    // Function to generate sorted user pair key
    def createSortedKey(friendA: Int, friendB: Int): String = {
      if (friendA < friendB) s"$friendA,$friendB" else s"$friendB,$friendA"
    }

    // Generate user pairs and record mutual friends
    val friendPairsRDD: RDD[(String, (Int, List[Int]))] = userFriendsRDD.flatMap { case (userA, friends) =>
      for {
        i <- friends.indices
        j <- i + 1 until friends.length
      } yield {
        val friendB = friends(i)
        val friendC = friends(j)
        val pair = createSortedKey(friendB, friendC)
        (pair, (userA, friends))
      }
    }

    // Add a print to check if pairs are being generated correctly
    println("Generated friendPairsRDD:")
    friendPairsRDD.take(10).foreach(println)

    // Group by friend pairs and find mutual friends
    val mutualFriendsRDD: RDD[(String, Iterable[(Int, List[Int])])] = friendPairsRDD.groupByKey()

    // Add a print to check if grouping is working
    println("Grouped mutualFriendsRDD:")
    mutualFriendsRDD.take(10).foreach(println)

    // Check for triadic closure by ensuring mutual friends are also directly connected
    val triadicClosureRDD: RDD[String] = mutualFriendsRDD.flatMap { case (pair, mutualFriendsData) =>
      val friends = pair.split(",")
      val friendB = friends(0).toInt
      val friendC = friends(1).toInt

      // For each mutual friend, check if friendB and friendC are directly connected
      mutualFriendsData.flatMap { case (userA, friendsOfA) =>
        if (!friendsOfA.contains(friendB) || !friendsOfA.contains(friendC)) {
          Some(s"($userA, $friendB, $friendC) -> Triadic closure not satisfied ($friendB and $friendC are not connected)")
        } else {
          None
        }
      }
    }

    // Add a print to check if the triadic closure results are generated
    println("Triadic closure results:")
    triadicClosureRDD.collect().foreach(println)

    // Save the output to HDFS as a single file
    triadicClosureRDD.coalesce(1).saveAsTextFile(outputHDFS)

    // End time in milliseconds
    val endTime = System.currentTimeMillis()

    // Calculate and print the duration in milliseconds
    val duration = endTime - startTime
    println(s"Task completed in $duration milliseconds")

    // Stop the Spark context
    sc.stop()
  }
}
