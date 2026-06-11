import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

object Main {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("RedditNER")
      .master("local[*]")
      .getOrCreate()
      
    val sc = spark.sparkContext

    // Parse command-line arguments
    val cmdArgs = CommandLineArgs.parse(args) match {
      case Some(parsed) => parsed
      case None => return // scopt prints error messages
    }

    // Load subscriptions
    val subscriptionOpts = FileIO.readSubscriptions(cmdArgs.subscriptionFile)

    // Filter out malformed subscriptions (None values)
    val subscriptions = subscriptionOpts.flatten

    if (subscriptions.isEmpty) {
      println("Error: No valid subscriptions found")
      return
    }

    //2a: cargar las suscripciones en un RDD
    val subscriptionsRDD = sc.parallelize(subscriptions)

    // Download feeds and parse posts, tracking success/failure
      val downloadResults = subscriptionsRDD.map { subscription =>
      val feedOpt = FileIO.downloadFeed(subscription.url)
      val posts = feedOpt.fold(List[Post]())(JsonParser.parsePosts(_, subscription.name))
      (feedOpt.isDefined, posts)
    }

    // Count feed successes/failures
    val feedsSuccess = downloadResults.filter(_._1).count()
    val feedsFailed = downloadResults.count() - feedsSuccess

    // Flatten all posts and count JSON parse failures
    val allPosts = downloadResults.flatMap(_._2)
    val postsSuccess = allPosts.count()
    val postsFailed = downloadResults.filter(_._2.isEmpty).count() 

    // Filter empty posts
    val filteredPosts = Analyzer.filterEmptyPosts(allPosts)
    val postsFiltered = allPosts.count() - filteredPosts.count()

    // Calculate average characters in filtered posts
    val totalChars = filteredPosts.map(post => post.title.length + post.selftext.length).sum
    val avgChars = if (filteredPosts.count > 0) totalChars / filteredPosts.count() else 0

    // Prepare statistics
    val stats = Map(
      "feedsSuccess" -> feedsSuccess.toInt,
      "feedsFailed" -> feedsFailed.toInt,
      "postsSuccess" -> postsSuccess.toInt,
      "postsFailed" -> postsFailed.toInt,
      "postsFiltered" -> postsFiltered.toInt,
      "avgChars" -> avgChars.toInt
    )

    // Print output
    println(Formatters.formatProcessingStats(stats))
    println()

    // Check if we have any posts to process
    if (filteredPosts.count() == 0) {
      println("Error: No valid posts downloaded after filtering")
      spark.stop()
      return
    }

    // Load dictionaries
    val dictionary = Dictionary.loadAll(cmdArgs.entitiesDir)

    // Detect entities in all posts (combine title and selftext)
    val allEntities = filteredPosts.flatMap { post =>
      val combinedText = post.title + " " + post.selftext
      Analyzer.detectEntities(combinedText, dictionary)
    }

    // Count entities
    val entityCounts = Analyzer.countEntities(allEntities)
    val typeStats = Analyzer.countByType(allEntities)

    println(Formatters.formatTypeStats(typeStats))
    println()
    println(Formatters.formatEntityStats(entityCounts, cmdArgs.topK))

    spark.stop()
  }
}