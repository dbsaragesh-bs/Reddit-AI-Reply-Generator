package com.reddit.streaming

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory
import java.util.UUID

/**
 * Spark Structured Streaming Application (Scala)
 * 
 * Consumes live comments from Kafka, applies Scala preprocessing,
 * calls inference service in batches, writes results to HDFS and Kafka.
 * 
 * Key Design Decisions:
 * - foreachBatch for micro-batch processing
 * - Partition-based HTTP calls (NOT row-by-row)
 * - Checkpointing enabled for fault tolerance
 * - Typed Datasets where possible
 * - All preprocessing in Scala
 */
object StreamingApp {

  private val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    logger.info("=== Starting Spark Streaming Service (Scala) ===")

    // Configuration from environment
    val sparkMasterUrl = sys.env.getOrElse("SPARK_MASTER_URL", "spark://spark-master:7077")
    val kafkaBootstrap = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    val hdfsUrl = sys.env.getOrElse("HDFS_URL", "hdfs://hadoop-namenode:9000")
    val inferenceUrl = sys.env.getOrElse("INFERENCE_SERVICE_URL", "http://inference-service:8000")
    val checkpointDir = sys.env.getOrElse("CHECKPOINT_DIR", s"$hdfsUrl/checkpoints/streaming")
    val batchSize = sys.env.getOrElse("BATCH_SIZE", "50").toInt

    // Create Spark session
    implicit val spark: SparkSession = SparkSession.builder()
      .appName("RedditStreamingPreprocessor")
      .master(sparkMasterUrl)
      .config("spark.sql.streaming.checkpointLocation", checkpointDir)
      .config("spark.sql.shuffle.partitions", "6")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.streaming.kafka.maxRatePerPartition", "1000")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.hadoop.fs.defaultFS", hdfsUrl)
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    logger.info(s"Spark session created. Master: $sparkMasterUrl")
    logger.info(s"Kafka: $kafkaBootstrap, HDFS: $hdfsUrl, Inference: $inferenceUrl")

    // Read from Kafka
    val kafkaStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrap)
      .option("subscribe", "comments_topic")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .option("kafka.group.id", "spark-streaming-consumer-group")
      .option("maxOffsetsPerTrigger", "10000")
      .load()

    logger.info("Kafka stream initialized on comments_topic")

    // Parse Kafka value as JSON
    val parsedStream = kafkaStream
      .selectExpr("CAST(value AS STRING) as json_str")
      .select(from_json(col("json_str"), PreprocessingEngine.commentSchema).as("data"))
      .select("data.*")

    // Apply foreachBatch processing
    val query: StreamingQuery = parsedStream.writeStream
      .foreachBatch { (batchDf: DataFrame, batchId: Long) =>
        processBatch(batchDf, batchId, hdfsUrl, inferenceUrl, kafkaBootstrap, batchSize)
      }
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .option("checkpointLocation", s"$checkpointDir/comments")
      .start()

    logger.info("Streaming query started. Awaiting termination...")

    // Graceful shutdown hook
    sys.addShutdownHook {
      logger.info("Shutdown signal received. Stopping streaming query...")
      query.stop()
      spark.stop()
      logger.info("Spark streaming stopped gracefully.")
    }

    query.awaitTermination()
  }

  /**
   * Process each micro-batch:
   * 1. Run Scala preprocessing pipeline
   * 2. Write cleaned data to HDFS
   * 3. Call inference service in batches per partition
   * 4. Write replies to HDFS
   * 5. Publish replies to Kafka replies_topic
   */
  private def processBatch(
    batchDf: DataFrame,
    batchId: Long,
    hdfsUrl: String,
    inferenceUrl: String,
    kafkaBootstrap: String,
    batchSize: Int
  )(implicit spark: SparkSession): Unit = {

    if (batchDf.isEmpty) {
      logger.debug(s"Batch $batchId is empty, skipping.")
      return
    }

    val startTime = System.currentTimeMillis()
    logger.info(s"=== Processing Batch $batchId with ${batchDf.count()} records ===")

    import spark.implicits._

    try {
      // STEP 1: Run Scala preprocessing pipeline
      val preprocessed = PreprocessingEngine.runPipeline(batchDf)
      val cleanedDs = PreprocessingEngine.selectCleanedColumns(preprocessed)
      val cleaned: DataFrame = cleanedDs.toDF()

      val cleanedCount = cleaned.count()
      logger.info(s"Batch $batchId: $cleanedCount records after preprocessing")

      if (cleanedCount == 0) {
        logger.warn(s"Batch $batchId: No records survived preprocessing")
        return
      }

      // STEP 2: Write cleaned data to HDFS
      val timestamp = System.currentTimeMillis()
      cleaned.write
        .mode("append")
        .json(s"$hdfsUrl/data/cleaned/streaming/batch_${batchId}_$timestamp")
      logger.info(s"Batch $batchId: Cleaned data written to HDFS")

      // STEP 3: Build a lookup map for post metadata (broadcast to executors)
      // This avoids the critical anti-pattern of querying a DataFrame inside mapPartitionsWithIndex
      val metadataMap: Map[String, (String, String, String, String, String, String)] = cleaned.collect().map { row =>
        val commentId = row.getAs[String]("commentId")
        val postId = row.getAs[String]("postId")
        val userId = row.getAs[String]("userId")
        val postText = row.getAs[String]("postText")
        val commentText = row.getAs[String]("commentText")
        val imageCaption = row.getAs[String]("imageCaption")
        val subreddit = row.getAs[String]("subreddit")
        commentId -> (postId, userId, postText, commentText, imageCaption, subreddit)
      }.toMap
      val broadcastMeta = spark.sparkContext.broadcast(metadataMap)

      // Call inference per partition using mapPartitionsWithIndex
      import org.apache.spark.sql.Row
      val repliesRdd = cleaned.rdd.mapPartitionsWithIndex { case (partitionIdx, iter) =>
        val partitionStart = System.currentTimeMillis()
        val records = iter.toSeq

        if (records.isEmpty) {
          Iterator.empty
        } else {
          // Batch records for HTTP calls
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

            InferenceClient.batchInfer(inferenceUrl, items)
          }

          val partitionLatency = System.currentTimeMillis() - partitionStart
          logger.info(s"Partition $partitionIdx: processed ${records.size} records in ${partitionLatency}ms")

          allReplies.iterator
        }
      }

      // Convert RDD to DataFrame using the broadcast metadata map (no distributed queries)
      val meta = broadcastMeta
      val repliesDf = repliesRdd.map { case (commentId, reply, latency) =>
        val metaTuple = meta.value.getOrElse(commentId, ("unknown", "unknown", "", "", "", "general"))
        GeneratedReply(
          replyId = UUID.randomUUID().toString,
          commentId = commentId,
          postId = metaTuple._1,
          userId = metaTuple._2,
          postText = metaTuple._3,
          commentText = metaTuple._4,
          generatedReply = reply,
          subreddit = metaTuple._6,
          inferenceLatencyMs = latency,
          generatedAt = System.currentTimeMillis()
        )
      }.toDS()

      if (!repliesDf.isEmpty) {
        // STEP 4: Write replies to HDFS
        repliesDf.write
          .mode("append")
          .json(s"$hdfsUrl/data/replies/streaming/batch_${batchId}_$timestamp")
        logger.info(s"Batch $batchId: ${repliesDf.count()} replies written to HDFS")

        // STEP 5: Publish replies to Kafka replies_topic
        repliesDf.select(
          col("replyId").as("key"),
          to_json(struct(
            col("replyId"),
            col("commentId"),
            col("postId"),
            col("generatedReply"),
            col("generatedAt").as("timestamp")
          )).as("value")
        ).write
          .format("kafka")
          .option("kafka.bootstrap.servers", kafkaBootstrap)
          .option("topic", "replies_topic")
          .save()
        logger.info(s"Batch $batchId: Replies published to Kafka replies_topic")
      }

      val totalLatency = System.currentTimeMillis() - startTime
      logger.info(s"=== Batch $batchId completed in ${totalLatency}ms ===")

    } catch {
      case e: Exception =>
        logger.error(s"Error processing batch $batchId: ${e.getMessage}", e)
        // Log failed batch for recovery
        logger.error(s"Failed batch $batchId details - records may need reprocessing")
    }
  }
}
