package com.reddit.batch

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.util.LongAccumulator
import org.slf4j.LoggerFactory
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import com.sun.net.httpserver.{HttpServer, HttpExchange, HttpHandler}
import java.net.InetSocketAddress
import com.google.gson.{Gson, JsonParser}

/**
 * Spark Batch Processing Application (Scala)
 * 
 * Reads bulk JSON from HDFS, applies preprocessing, calls inference,
 * writes enriched results back to HDFS.
 * 
 * Exposes a simple HTTP endpoint for triggering batch jobs.
 * 
 * Key Design:
 * - Partition-based processing (avoids driver OOM)
 * - Batch HTTP calls to inference service
 * - Intelligent repartitioning for 50k records
 * - Per-partition execution time logging
 */
object BatchApp {

  private val logger = LoggerFactory.getLogger(this.getClass)
  private val gson = new Gson()
  private val jobStatuses = new ConcurrentHashMap[String, BatchJobStatus]()

  def main(args: Array[String]): Unit = {
    logger.info("=== Starting Spark Batch Service (Scala) ===")

    val sparkMasterUrl = sys.env.getOrElse("SPARK_MASTER_URL", "spark://spark-master:7077")
    val hdfsUrl = sys.env.getOrElse("HDFS_URL", "hdfs://hadoop-namenode:9000")
    val inferenceUrl = sys.env.getOrElse("INFERENCE_SERVICE_URL", "http://inference-service:8000")
    val batchSize = sys.env.getOrElse("BATCH_SIZE", "50").toInt
    val httpPort = sys.env.getOrElse("HTTP_PORT", "8085").toInt

    implicit val spark: SparkSession = SparkSession.builder()
      .appName("RedditBatchPreprocessor")
      .master(sparkMasterUrl)
      .config("spark.sql.shuffle.partitions", "12")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.hadoop.fs.defaultFS", hdfsUrl)
      .config("spark.driver.maxResultSize", "1g")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    logger.info(s"Spark batch session created. Master: $sparkMasterUrl")

    // Start HTTP server for job triggers
    startHttpServer(httpPort, hdfsUrl, inferenceUrl, batchSize)

    // Keep alive
    logger.info(s"Batch service HTTP server running on port $httpPort")
    Thread.currentThread().join()
  }

  /**
   * Simple HTTP server to trigger batch jobs and check status.
   */
  private def startHttpServer(
    port: Int,
    hdfsUrl: String,
    inferenceUrl: String,
    batchSize: Int
  )(implicit spark: SparkSession): Unit = {

    val server = HttpServer.create(new InetSocketAddress(port), 0)

    // POST /process - Trigger a batch job
    server.createContext("/process", new HttpHandler {
      override def handle(exchange: HttpExchange): Unit = {
        if (exchange.getRequestMethod == "POST") {
          try {
            val body = new String(exchange.getRequestBody.readAllBytes())
            // Use instance method parse() instead of static parseString() for Gson 2.2.x compatibility
            val json = new JsonParser().parse(body).getAsJsonObject
            val inputPath = json.get("input_path").getAsString
            val outputPath = json.get("output_path").getAsString

            val jobId = UUID.randomUUID().toString
            logger.info(s"Received batch job request: $jobId, input: $inputPath, output: $outputPath")

            // Run job in background
            new Thread(() => {
              processBatchJob(jobId, s"$hdfsUrl$inputPath", s"$hdfsUrl$outputPath", inferenceUrl, batchSize)
            }).start()

            val response = s"""{"job_id": "$jobId", "status": "submitted"}"""
            exchange.getResponseHeaders.set("Content-Type", "application/json")
            exchange.sendResponseHeaders(200, response.getBytes.length)
            exchange.getResponseBody.write(response.getBytes)
            exchange.getResponseBody.close()
          } catch {
            case e: Throwable =>
              logger.error(s"Error processing batch request: ${e.getMessage}", e)
              val error = s"""{"error": "${e.getMessage.replace("\"", "'")}"}"""
              exchange.getResponseHeaders.set("Content-Type", "application/json")
              exchange.sendResponseHeaders(500, error.getBytes.length)
              exchange.getResponseBody.write(error.getBytes)
              exchange.getResponseBody.close()
          }
        }
      }
    })

    // GET /status/{jobId} - Check job status
    server.createContext("/status/", new HttpHandler {
      override def handle(exchange: HttpExchange): Unit = {
        val jobId = exchange.getRequestURI.getPath.replace("/status/", "")
        val status = jobStatuses.get(jobId)

        val response = if (status != null) {
          gson.toJson(status)
        } else {
          s"""{"error": "Job not found: $jobId"}"""
        }

        exchange.getResponseHeaders.set("Content-Type", "application/json")
        exchange.sendResponseHeaders(200, response.getBytes.length)
        exchange.getResponseBody.write(response.getBytes)
        exchange.getResponseBody.close()
      }
    })

    // GET /health
    server.createContext("/health", new HttpHandler {
      override def handle(exchange: HttpExchange): Unit = {
        val response = """{"status": "healthy", "service": "spark-batch-service"}"""
        exchange.getResponseHeaders.set("Content-Type", "application/json")
        exchange.sendResponseHeaders(200, response.getBytes.length)
        exchange.getResponseBody.write(response.getBytes)
        exchange.getResponseBody.close()
      }
    })

    server.setExecutor(null)
    server.start()
  }

  /**
   * Process a batch job:
   * 1. Read bulk JSON from HDFS
   * 2. Run preprocessing pipeline (Scala)
   * 3. Call inference per partition (batched)
   * 4. Write enriched results to HDFS
   */
  private def processBatchJob(
    jobId: String,
    inputPath: String,
    outputPath: String,
    inferenceUrl: String,
    batchSize: Int
  )(implicit spark: SparkSession): Unit = {

    val startTime = System.currentTimeMillis()
    import spark.implicits._

    jobStatuses.put(jobId, BatchJobStatus(
      jobId, "processing", inputPath, outputPath, 0, 0, startTime, 0, None
    ))

    try {
      logger.info(s"=== Batch Job $jobId: Reading from $inputPath ===")

      // Read raw JSON (multiLine=true for JSON array format)
      val rawDf = spark.read
        .schema(BatchPreprocessingEngine.bulkPostSchema)
        .option("multiLine", "true")
        .json(inputPath)

      val totalPosts = rawDf.count()
      logger.info(s"Job $jobId: Read $totalPosts posts from HDFS")

      // Run all preprocessing (SCALA ONLY)
      val preprocessed = BatchPreprocessingEngine.runPipeline(rawDf)
      val cleaned: DataFrame = BatchPreprocessingEngine.selectFinalColumns(preprocessed)

      val totalRecords = cleaned.count()
      logger.info(s"Job $jobId: $totalRecords records after preprocessing")

      // Accumulator for incremental progress tracking
      val processedAccumulator: LongAccumulator = spark.sparkContext.longAccumulator("processedRecords")

      jobStatuses.put(jobId, BatchJobStatus(
        jobId, "inferring", inputPath, outputPath, totalRecords, 0, startTime, 0, None
      ))

      // Background thread to periodically sync accumulator -> jobStatuses
      @volatile var progressRunning = true
      val progressThread = new Thread(() => {
        while (progressRunning) {
          try {
            Thread.sleep(2000)
            val current = processedAccumulator.value
            val existing = jobStatuses.get(jobId)
            if (existing != null && existing.status == "inferring" && current != existing.processedRecords) {
              jobStatuses.put(jobId, existing.copy(processedRecords = current))
              logger.info(s"Job $jobId: Progress $current / $totalRecords")
            }
          } catch {
            case _: InterruptedException => // exit
          }
        }
      })
      progressThread.setDaemon(true)
      progressThread.start()

      // Repartition into small chunks so accumulator updates frequently
      // Each partition = ~batchSize records, giving granular progress
      val numPartitions = Math.max(1, (totalRecords / batchSize).toInt + 1)
      val repartitioned = cleaned.repartition(numPartitions)
      logger.info(s"Job $jobId: Repartitioned into $numPartitions partitions (batchSize=$batchSize)")

      // Process per partition with batch inference calls
      val repliesRdd = repartitioned.rdd.mapPartitionsWithIndex { case (partIdx, iter) =>
        val partStart = System.currentTimeMillis()
        val records = iter.toSeq

        if (records.isEmpty) {
          Iterator.empty
        } else {
          val batches = records.grouped(batchSize).toSeq
          val allReplies = batches.flatMap { batch =>
            val items = batch.map { row =>
              (
                row.getAs[String]("commentId"),
                row.getAs[String]("postText"),
                row.getAs[String]("commentText"),
                row.getAs[String]("imageCaption")
              )
            }
            val results = BatchInferenceClient.batchInfer(inferenceUrl, items)
            processedAccumulator.add(results.size.toLong)
            results
          }

          val partLatency = System.currentTimeMillis() - partStart
          logger.info(s"Job $jobId | Partition $partIdx: ${records.size} records processed in ${partLatency}ms")

          allReplies.iterator
        }
      }

      // Force materialization of inference results so accumulator updates incrementally
      // Each partition completion sends accumulator deltas back to driver
      val cachedRepliesRdd = repliesRdd.cache()
      val replyCount = cachedRepliesRdd.count()
      logger.info(s"Job $jobId: Inference complete. $replyCount replies generated.")

      // Build final enriched dataset
      val repliesDf = cachedRepliesRdd.map { case (commentId, reply, latency) =>
        BatchReply(
          replyId = UUID.randomUUID().toString,
          commentId = commentId,
          postId = "",
          postTitle = "",
          postText = "",
          commentText = "",
          generatedReply = reply,
          subreddit = "",
          inferenceLatencyMs = latency,
          processedAt = System.currentTimeMillis()
        )
      }.toDS()

      // Join with original data for enrichment
      val enriched = cleaned.as("c")
        .join(repliesDf.as("r"), col("c.commentId") === col("r.commentId"), "left")
        .select(
          col("c.commentId"),
          col("c.postId"),
          col("c.userId"),
          col("c.username"),
          col("c.postTitle"),
          col("c.postText"),
          col("c.commentText"),
          col("c.imageCaption"),
          col("c.subreddit"),
          col("c.timestamp"),
          col("r.generatedReply"),
          col("r.inferenceLatencyMs"),
          col("r.processedAt")
        )

      // Write to HDFS
      enriched.write
        .mode("overwrite")
        .json(outputPath)

      // Unpersist cached RDD
      cachedRepliesRdd.unpersist()

      // Stop progress tracking thread
      progressRunning = false
      progressThread.interrupt()

      val endTime = System.currentTimeMillis()

      logger.info(s"=== Batch Job $jobId COMPLETE: $replyCount records in ${endTime - startTime}ms ===")
      logger.info(s"Output written to: $outputPath")

      jobStatuses.put(jobId, BatchJobStatus(
        jobId, "completed", inputPath, outputPath, totalRecords, replyCount, startTime, endTime, None
      ))

    } catch {
      case e: Exception =>
        val endTime = System.currentTimeMillis()
        logger.error(s"Batch Job $jobId FAILED: ${e.getMessage}", e)
        jobStatuses.put(jobId, BatchJobStatus(
          jobId, "failed", inputPath, outputPath, 0, 0, startTime, endTime, Some(e.getMessage)
        ))
    }
  }
}
