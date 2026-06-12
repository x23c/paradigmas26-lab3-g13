import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import scala.util.{Try, Success, Failure}

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
    // Acumuladores — se declaran en el driver
    val feedsOk     = sc.longAccumulator("feedsOk")
    val feedsFail   = sc.longAccumulator("feedsFail")
    val postsTotal  = sc.longAccumulator("postsTotal")
    val postsFilt   = sc.longAccumulator("postsFiltered")
    val postsParseFail = sc.longAccumulator("postsParseFail")

    //2a: cargar las suscripciones en un RDD
    val subscriptionsRDD = sc.parallelize(subscriptions)

    // Download feeds and parse posts, tracking success/failure
    val filteredPosts = subscriptionsRDD.flatMap { sub =>
      Try(FileIO.downloadFeed(sub.url)) match {
        case Failure(_) =>
          feedsFail.add(1)
          println(s"Warning: Failed to download from '${sub.name}' (${sub.url})")
          Iterator.empty
        case Success(None) =>
          feedsFail.add(1)
          println(s"Warning: Failed to download from '${sub.name}' (${sub.url})")
          Iterator.empty
        case Success(Some(feed)) =>
          feedsOk.add(1)
          val posts = Try(JsonParser.parsePosts(feed, sub.name)).getOrElse {
            postsParseFail.add(1)
            println(s"Warning: Failed to parse posts from '${sub.name}' (${sub.url})")
            List.empty
          }
          postsTotal.add(posts.length)
          val validos = posts.filter(p => p.title.nonEmpty && p.selftext.nonEmpty && p.selftext.trim.nonEmpty)
          postsFilt.add(posts.length - validos.length)
          validos.iterator
      }
    }.cache()
    // ejecuto la funcion
    val totalPosts = filteredPosts.count()

    // Count feed successes/failures
    val feedsSuccess = feedsOk.value
    val feedsFailed = feedsFail.value

    // Flatten all posts and count JSON parse failures
    val postsSuccess = postsTotal.value
    val postsFailed = postsParseFail.value

    // Filter empty posts
    val postsFiltered = postsFilt.value
    // Calculate average characters in filtered posts
    val avgChars = if (totalPosts > 0)
      filteredPosts.map(p => p.title.length + p.selftext.length).sum().toLong / totalPosts
    else 0

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
    if (totalPosts == 0) {
      println("Error: No valid posts downloaded after filtering")
      //libero memoria
      filteredPosts.unpersist()
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
   // val entityCounts = Analyzer.countEntities(allEntities)
    val mappedEntities = Analyzer.transformRDD(allEntities)
    val countedEntities = Analyzer.countInstancesOf(mappedEntities)
    val sortedEntities = Analyzer.ordenarDescendente(countedEntities)
    sortedEntities.collect().foreach {
          case ((tipo, entidad), cantidad) =>
            println(s"[$tipo] $entidad: $cantidad apariciones")
          }
    val entityCounts = sortedEntities.collect().toMap

    val typeStats = Analyzer.countByType(allEntities)

    println(Formatters.formatTypeStats(typeStats))
    println()
    println(Formatters.formatEntityStats(entityCounts, cmdArgs.topK))
    //Libero memoria
    filteredPosts.unpersist()
    spark.stop()
  }
}