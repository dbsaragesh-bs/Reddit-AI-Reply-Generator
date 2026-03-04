package com.reddit.streaming

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

/**
 * Scala Preprocessing Engine
 * 
 * ALL preprocessing logic is implemented here in Scala:
 * - Schema validation
 * - Data cleaning (emoji removal, special chars, whitespace normalization)
 * - Null handling
 * - Deduplication
 * - Image metadata enrichment
 * - Feature structuring
 * 
 * NO preprocessing logic exists in Python services.
 */
object PreprocessingEngine {

  private val logger = LoggerFactory.getLogger(this.getClass)

  // Regex patterns for text cleaning
  private val EMOJI_PATTERN = "[\\x{1F600}-\\x{1F64F}\\x{1F300}-\\x{1F5FF}\\x{1F680}-\\x{1F6FF}\\x{1F1E0}-\\x{1F1FF}\\x{2600}-\\x{26FF}\\x{2700}-\\x{27BF}\\x{FE00}-\\x{FE0F}\\x{1F900}-\\x{1F9FF}\\x{200D}\\x{20E3}\\x{E0020}-\\x{E007F}]+"
  private val SPECIAL_CHARS_PATTERN = "[^a-zA-Z0-9\\s.,!?'\"@#$%&*()\\-_+=;:<>/\\\\\\[\\]{}|~`]"
  private val MULTIPLE_SPACES_PATTERN = "\\s+"
  private val URL_PATTERN = "(https?://\\S+|www\\.\\S+)"

  /** Raw JSON schema for incoming Kafka messages */
  val commentSchema: StructType = StructType(Seq(
    StructField("commentId", StringType, nullable = true),
    StructField("postId", StringType, nullable = true),
    StructField("userId", StringType, nullable = true),
    StructField("username", StringType, nullable = true),
    StructField("postText", StringType, nullable = true),
    StructField("commentText", StringType, nullable = true),
    StructField("imageUrl", StringType, nullable = true),
    StructField("imageCaption", StringType, nullable = true),
    StructField("subreddit", StringType, nullable = true),
    StructField("timestamp", LongType, nullable = true),
    StructField("parentCommentId", StringType, nullable = true),
    StructField("upvotes", IntegerType, nullable = true),
    StructField("metadata", MapType(StringType, StringType), nullable = true)
  ))

  /**
   * STEP 1: Schema Validation
   * Validates that required fields are present and correctly typed.
   */
  def validateSchema(df: Dataset[_])(implicit spark: SparkSession): Dataset[_] = {
    import spark.implicits._
    logger.info("Performing schema validation...")

    val validated = df
      .filter(col("commentId").isNotNull && col("commentId") =!= "")
      .filter(col("postId").isNotNull && col("postId") =!= "")
      .filter(col("commentText").isNotNull && col("commentText") =!= "")
      .filter(col("postText").isNotNull && col("postText") =!= "")
      .filter(col("userId").isNotNull && col("userId") =!= "")

    val beforeCount = df.count()
    val afterCount = validated.count()
    logger.info(s"Schema validation: $beforeCount -> $afterCount records (dropped ${beforeCount - afterCount})")
    
    validated
  }

  /**
   * STEP 2: Clean Text
   * - Remove emojis
   * - Strip special characters  
   * - Normalize whitespace
   * - Remove URLs
   * - Trim text
   */
  def cleanText(df: Dataset[_])(implicit spark: SparkSession): Dataset[_] = {
    import spark.implicits._
    logger.info("Cleaning text fields...")

    df
      // Clean postText
      .withColumn("postText", regexp_replace(col("postText"), EMOJI_PATTERN, ""))
      .withColumn("postText", regexp_replace(col("postText"), URL_PATTERN, "[URL]"))
      .withColumn("postText", regexp_replace(col("postText"), SPECIAL_CHARS_PATTERN, ""))
      .withColumn("postText", regexp_replace(col("postText"), MULTIPLE_SPACES_PATTERN, " "))
      .withColumn("postText", trim(col("postText")))
      // Clean commentText
      .withColumn("commentText", regexp_replace(col("commentText"), EMOJI_PATTERN, ""))
      .withColumn("commentText", regexp_replace(col("commentText"), URL_PATTERN, "[URL]"))
      .withColumn("commentText", regexp_replace(col("commentText"), SPECIAL_CHARS_PATTERN, ""))
      .withColumn("commentText", regexp_replace(col("commentText"), MULTIPLE_SPACES_PATTERN, " "))
      .withColumn("commentText", trim(col("commentText")))
      // Clean username
      .withColumn("username", regexp_replace(col("username"), SPECIAL_CHARS_PATTERN, ""))
      .withColumn("username", trim(col("username")))
      // Clean subreddit
      .withColumn("subreddit", regexp_replace(col("subreddit"), SPECIAL_CHARS_PATTERN, ""))
      .withColumn("subreddit", lower(trim(col("subreddit"))))
      // Filter out empty text after cleaning
      .filter(length(col("commentText")) > 0)
      .filter(length(col("postText")) > 0)
  }

  /**
   * STEP 3: Handle null values
   * Replace nulls with sensible defaults.
   */
  def handleNulls(df: Dataset[_])(implicit spark: SparkSession): Dataset[_] = {
    logger.info("Handling null values...")

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
   * STEP 4: Deduplicate records
   * Remove duplicate comments based on commentId.
   */
  def deduplicate(df: Dataset[_])(implicit spark: SparkSession): Dataset[_] = {
    logger.info("Deduplicating records...")
    df.dropDuplicates("commentId")
  }

  /**
   * STEP 5: Enrich with image metadata
   * Add image-related features to the dataset.
   */
  def enrichImageMetadata(df: Dataset[_])(implicit spark: SparkSession): Dataset[_] = {
    logger.info("Enriching image metadata...")

    df
      .withColumn("hasImage", when(col("imageUrl") =!= "", true).otherwise(false))
      .withColumn("hasCaption", when(col("imageCaption") =!= "", true).otherwise(false))
      .withColumn("imageCaption",
        when(col("hasImage") && !col("hasCaption"), lit("[Image without caption]"))
          .otherwise(col("imageCaption"))
      )
  }

  /**
   * STEP 6: Structure features
   * Add computed features for downstream processing.
   */
  def structureFeatures(df: Dataset[_])(implicit spark: SparkSession): Dataset[_] = {
    import spark.implicits._
    logger.info("Structuring features...")

    df
      .withColumn("wordCount", size(split(col("commentText"), "\\s+")))
      .withColumn("charCount", length(col("commentText")))
      .withColumn("processedAt", lit(System.currentTimeMillis()))
      .withColumn("isReply", when(col("parentCommentId") =!= "", true).otherwise(false))
  }

  /**
   * Run the complete preprocessing pipeline.
   * All steps are executed in order on the DataFrame.
   */
  def runPipeline(df: Dataset[_])(implicit spark: SparkSession): Dataset[_] = {
    logger.info("=== Starting Scala Preprocessing Pipeline ===")
    val startTime = System.currentTimeMillis()

    val result = {
      val step1 = validateSchema(df)
      val step2 = cleanText(step1)
      val step3 = handleNulls(step2)
      val step4 = deduplicate(step3)
      val step5 = enrichImageMetadata(step4)
      val step6 = structureFeatures(step5)
      step6
    }

    val elapsed = System.currentTimeMillis() - startTime
    logger.info(s"=== Preprocessing Pipeline Complete in ${elapsed}ms ===")
    result
  }

  /**
   * Select only the columns needed for the CleanedComment case class.
   */
  def selectCleanedColumns(df: Dataset[_])(implicit spark: SparkSession): DataFrame = {
    df.select(
      col("commentId"),
      col("postId"),
      col("userId"),
      col("username"),
      col("postText"),
      col("commentText"),
      col("imageCaption"),
      col("subreddit"),
      col("timestamp"),
      col("parentCommentId"),
      col("upvotes"),
      col("wordCount"),
      col("charCount"),
      col("processedAt")
    )
  }
}
