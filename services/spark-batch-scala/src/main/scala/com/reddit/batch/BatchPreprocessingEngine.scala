package com.reddit.batch

import org.apache.spark.sql.{Dataset, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

/**
 * Batch Preprocessing Engine (Scala)
 * 
 * ALL preprocessing for batch jobs:
 * - Exploding nested JSON (posts → comments)
 * - Schema validation
 * - Text cleaning
 * - Null handling  
 * - Deduplication
 * - Image metadata enrichment
 * - Feature structuring
 * - Intelligent repartitioning
 */
object BatchPreprocessingEngine {

  private val logger = LoggerFactory.getLogger(this.getClass)

  private val EMOJI_PATTERN = "[\\x{1F600}-\\x{1F64F}\\x{1F300}-\\x{1F5FF}\\x{1F680}-\\x{1F6FF}\\x{1F1E0}-\\x{1F1FF}\\x{2600}-\\x{26FF}\\x{2700}-\\x{27BF}\\x{FE00}-\\x{FE0F}\\x{1F900}-\\x{1F9FF}\\x{200D}\\x{20E3}\\x{E0020}-\\x{E007F}]+"
  private val SPECIAL_CHARS_PATTERN = "[^a-zA-Z0-9\\s.,!?'\"@#$%&*()\\-_+=;:<>/\\\\\\[\\]{}|~`]"
  private val MULTIPLE_SPACES_PATTERN = "\\s+"
  private val URL_PATTERN = "(https?://\\S+|www\\.\\S+)"

  /** Schema for bulk JSON uploads */
  val bulkPostSchema: StructType = StructType(Seq(
    StructField("postId", StringType),
    StructField("userId", StringType),
    StructField("username", StringType),
    StructField("title", StringType),
    StructField("postText", StringType),
    StructField("subreddit", StringType),
    StructField("imageUrl", StringType, nullable = true),
    StructField("imageCaption", StringType, nullable = true),
    StructField("timestamp", LongType),
    StructField("upvotes", IntegerType),
    StructField("comments", ArrayType(StructType(Seq(
      StructField("commentId", StringType),
      StructField("userId", StringType),
      StructField("username", StringType),
      StructField("commentText", StringType),
      StructField("timestamp", LongType),
      StructField("upvotes", IntegerType),
      StructField("parentCommentId", StringType, nullable = true)
    )), containsNull = true), nullable = true)
  ))

  /**
   * STEP 1: Explode nested JSON
   * Flatten posts to individual comments with post context.
   */
  def explodeNestedJson(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    logger.info("Exploding nested JSON (posts → comments)...")

    val exploded = df
      .withColumn("comment", explode_outer(col("comments")))
      .select(
        col("comment.commentId").as("commentId"),
        col("postId"),
        col("comment.userId").as("userId"),
        col("comment.username").as("username"),
        col("title").as("postTitle"),
        col("postText"),
        col("comment.commentText").as("commentText"),
        col("imageUrl"),
        col("imageCaption"),
        col("subreddit"),
        col("comment.timestamp").as("timestamp"),
        col("comment.upvotes").as("upvotes"),
        col("comment.parentCommentId").as("parentCommentId")
      )
      .filter(col("commentId").isNotNull)

    logger.info(s"Exploded to ${exploded.count()} individual comment records")
    exploded
  }

  /**
   * STEP 2: Schema Validation
   */
  def validateSchema(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    logger.info("Validating schema for batch records...")

    val validated = df
      .filter(col("commentId").isNotNull && col("commentId") =!= "")
      .filter(col("postId").isNotNull && col("postId") =!= "")
      .filter(col("commentText").isNotNull && col("commentText") =!= "")
      .filter(col("postText").isNotNull && col("postText") =!= "")

    val before = df.count()
    val after = validated.count()
    logger.info(s"Schema validation: $before → $after (dropped ${before - after})")
    validated
  }

  /**
   * STEP 3: Clean text fields
   */
  def cleanText(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    logger.info("Cleaning text in batch records...")

    df
      .withColumn("postText", regexp_replace(col("postText"), EMOJI_PATTERN, ""))
      .withColumn("postText", regexp_replace(col("postText"), URL_PATTERN, "[URL]"))
      .withColumn("postText", regexp_replace(col("postText"), SPECIAL_CHARS_PATTERN, ""))
      .withColumn("postText", regexp_replace(col("postText"), MULTIPLE_SPACES_PATTERN, " "))
      .withColumn("postText", trim(col("postText")))
      .withColumn("postTitle", regexp_replace(col("postTitle"), EMOJI_PATTERN, ""))
      .withColumn("postTitle", regexp_replace(col("postTitle"), SPECIAL_CHARS_PATTERN, ""))
      .withColumn("postTitle", trim(col("postTitle")))
      .withColumn("commentText", regexp_replace(col("commentText"), EMOJI_PATTERN, ""))
      .withColumn("commentText", regexp_replace(col("commentText"), URL_PATTERN, "[URL]"))
      .withColumn("commentText", regexp_replace(col("commentText"), SPECIAL_CHARS_PATTERN, ""))
      .withColumn("commentText", regexp_replace(col("commentText"), MULTIPLE_SPACES_PATTERN, " "))
      .withColumn("commentText", trim(col("commentText")))
      .withColumn("username", regexp_replace(col("username"), SPECIAL_CHARS_PATTERN, ""))
      .withColumn("username", trim(col("username")))
      .withColumn("subreddit", lower(trim(regexp_replace(col("subreddit"), SPECIAL_CHARS_PATTERN, ""))))
      .filter(length(col("commentText")) > 0)
      .filter(length(col("postText")) > 0)
  }

  /**
   * STEP 4: Handle nulls
   */
  def handleNulls(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    logger.info("Handling nulls in batch records...")

    df
      .withColumn("imageUrl", coalesce(col("imageUrl"), lit("")))
      .withColumn("imageCaption", coalesce(col("imageCaption"), lit("")))
      .withColumn("parentCommentId", coalesce(col("parentCommentId"), lit("")))
      .withColumn("username", coalesce(col("username"), lit("anonymous")))
      .withColumn("subreddit", coalesce(col("subreddit"), lit("general")))
      .withColumn("upvotes", coalesce(col("upvotes"), lit(0)))
      .withColumn("timestamp", coalesce(col("timestamp"), lit(System.currentTimeMillis())))
  }

  /**
   * STEP 5: Deduplicate
   */
  def deduplicate(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    logger.info("Deduplicating batch records...")
    df.dropDuplicates("commentId")
  }

  /**
   * STEP 6: Enrich image metadata
   */
  def enrichImageMetadata(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    logger.info("Enriching image metadata for batch...")

    df
      .withColumn("hasImage", when(col("imageUrl") =!= "", true).otherwise(false))
      .withColumn("hasCaption", when(col("imageCaption") =!= "", true).otherwise(false))
      .withColumn("imageCaption",
        when(col("hasImage") && !col("hasCaption"), lit("[Image without caption]"))
          .otherwise(col("imageCaption"))
      )
  }

  /**
   * STEP 7: Structure features  
   */
  def structureFeatures(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    logger.info("Structuring features for batch...")

    df
      .withColumn("wordCount", size(split(col("commentText"), "\\s+")))
      .withColumn("charCount", length(col("commentText")))
      .withColumn("processedAt", lit(System.currentTimeMillis()))
  }

  /**
   * STEP 8: Intelligent repartitioning
   * Repartition based on data size to avoid OOM and maximize parallelism.
   */
  def intelligentRepartition(df: DataFrame, targetPartitionSize: Int = 500)(implicit spark: SparkSession): DataFrame = {
    val totalRecords = df.count()
    val numPartitions = Math.max(1, Math.ceil(totalRecords.toDouble / targetPartitionSize).toInt)
    logger.info(s"Repartitioning: $totalRecords records → $numPartitions partitions (target: $targetPartitionSize per partition)")
    df.repartition(numPartitions)
  }

  /**
   * Run the complete batch preprocessing pipeline.
   */
  def runPipeline(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    logger.info("=== Starting Batch Scala Preprocessing Pipeline ===")
    val startTime = System.currentTimeMillis()

    val result = {
      val step1 = explodeNestedJson(df)
      val step2 = validateSchema(step1)
      val step3 = cleanText(step2)
      val step4 = handleNulls(step3)
      val step5 = deduplicate(step4)
      val step6 = enrichImageMetadata(step5)
      val step7 = structureFeatures(step6)
      val step8 = intelligentRepartition(step7)
      step8
    }

    val elapsed = System.currentTimeMillis() - startTime
    logger.info(s"=== Batch Preprocessing Pipeline Complete in ${elapsed}ms ===")
    result
  }

  /**
   * Select final columns for inference.
   */
  def selectFinalColumns(df: DataFrame): DataFrame = {
    df.select(
      col("commentId"),
      col("postId"),
      col("userId"),
      col("username"),
      col("postTitle"),
      col("postText"),
      col("commentText"),
      col("imageCaption"),
      col("subreddit"),
      col("timestamp"),
      col("upvotes"),
      col("wordCount"),
      col("charCount"),
      col("processedAt")
    )
  }
}
