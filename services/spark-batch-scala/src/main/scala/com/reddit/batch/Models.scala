package com.reddit.batch

/**
 * Domain models for the Spark Batch Processing Service.
 */

/** Raw post from bulk JSON upload */
case class RawPost(
  postId: String,
  userId: String,
  username: String,
  title: String,
  postText: String,
  subreddit: String,
  imageUrl: Option[String],
  imageCaption: Option[String],
  timestamp: Long,
  upvotes: Int,
  comments: Option[Seq[RawBulkComment]]
)

/** Raw comment nested in post */
case class RawBulkComment(
  commentId: String,
  userId: String,
  username: String,
  commentText: String,
  timestamp: Long,
  upvotes: Int,
  parentCommentId: Option[String]
)

/** Exploded and cleaned comment for inference */
case class CleanedBulkRecord(
  commentId: String,
  postId: String,
  userId: String,
  username: String,
  postTitle: String,
  postText: String,
  commentText: String,
  imageCaption: String,
  subreddit: String,
  timestamp: Long,
  upvotes: Int,
  wordCount: Int,
  charCount: Int,
  processedAt: Long
)

/** Inference request item */
case class InferenceItem(
  id: String,
  post_text: String,
  comment_text: String,
  image_caption: String
)

/** Reply from inference */
case class BatchReply(
  replyId: String,
  commentId: String,
  postId: String,
  postTitle: String,
  postText: String,
  commentText: String,
  generatedReply: String,
  subreddit: String,
  inferenceLatencyMs: Long,
  processedAt: Long
)

/** Batch job status */
case class BatchJobStatus(
  jobId: String,
  status: String,
  inputPath: String,
  outputPath: String,
  totalRecords: Long,
  processedRecords: Long,
  startTime: Long,
  endTime: Long,
  error: Option[String]
)
