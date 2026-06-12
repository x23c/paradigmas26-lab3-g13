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

    // PIPELINE 1: Descargar feeds, parsear posts y filtrado 
    val startPipeline1 = System.currentTimeMillis()

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

    val endPipeline1 = System.currentTimeMillis()
    val durationPipeline1 = (endPipeline1 - startPipeline1) / 1000.0
    println(f"Tiempo de descarga, parseo y filtrado: $durationPipeline1%.3f segundos")
    println()
    // PIPELINE 1 end

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

    // PIPELINE 2: Carga de diccionarios y detección de entidades
    val startPipeline2 = System.currentTimeMillis()

    // Load dictionaries
    val dictionary = Dictionary.loadAll(cmdArgs.entitiesDir)

    // Detect entities in all posts (combine title and selftext)
    val allEntities = filteredPosts.flatMap { post =>
      val combinedText = post.title + " " + post.selftext
      Analyzer.detectEntities(combinedText, dictionary)
    }.cache()

    // Count entities
    val entityCounts = Analyzer.countEntities(allEntities)
    val typeStats = Analyzer.countByType(allEntities)

    allEntities.unpersist()

    val endPipeline2 = System.currentTimeMillis()
    val durationPipeline2 = (endPipeline2 - startPipeline2) / 1000.0
    println(f"Tiempo de detección y conteo de entidades: $durationPipeline2%.3f segundos")
    println()
    // PIPELINE 2 end

    println(Formatters.formatTypeStats(typeStats))
    println()
    println(Formatters.formatEntityStats(entityCounts, cmdArgs.topK))
    //Libero memoria
    filteredPosts.unpersist()
    spark.stop()
  }
}