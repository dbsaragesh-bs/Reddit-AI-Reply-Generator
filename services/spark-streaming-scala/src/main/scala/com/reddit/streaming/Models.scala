package com.reddit.streaming

/**
 * Domain models for the Social Media Reply Generation System.
 * All case classes used for typed Datasets in Spark.
 */

/** Raw comment received from Kafka */
case class RawComment(
  commentId: String,
  postId: String,
  userId: String,
  username: String,
  postText: String,
  commentText: String,
  imageUrl: Option[String],
  imageCaption: Option[String],
  subreddit: String,
  timestamp: Long,
  parentCommentId: Option[String],
  upvotes: Int,
  metadata: Option[Map[String, String]]
)

/** Cleaned and validated comment after Scala preprocessing */
case class CleanedComment(
  commentId: String,
  postId: String,
  userId: String,
  username: String,
  postText: String,
  commentText: String,
  imageCaption: String,
  subreddit: String,
  timestamp: Long,
  parentCommentId: String,
  upvotes: Int,
  wordCount: Int,
  charCount: Int,
  processedAt: Long
)

/** Batch inference request sent to the inference service */
case class InferenceRequest(
  post_text: String,
  comments: Seq[String],
  image_caption: String
)

/** Single item in inference request */
case class InferenceItem(
  id: String,
  post_text: String,
  comment_text: String,
  image_caption: String
)

/** Batch request wrapper */
case class BatchInferenceRequest(
  items: Seq[InferenceItem]
)

/** Response from inference service */
case class InferenceResponse(
  id: String,
  generated_reply: String,
  latency_ms: Long
)

/** Batch response wrapper */
case class BatchInferenceResponse(
  replies: Seq[InferenceResponse]
)

/** Generated reply record for HDFS storage */
case class GeneratedReply(
  replyId: String,
  commentId: String,
  postId: String,
  userId: String,
  postText: String,
  commentText: String,
  generatedReply: String,
  subreddit: String,
  inferenceLatencyMs: Long,
  generatedAt: Long
)

/** Kafka reply message for replies_topic */
case class KafkaReplyMessage(
  replyId: String,
  commentId: String,
  postId: String,
  generatedReply: String,
  timestamp: Long
)
