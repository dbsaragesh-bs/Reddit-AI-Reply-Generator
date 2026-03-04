package com.reddit.streaming

import scalaj.http.{Http, HttpOptions}
import com.google.gson.{Gson, GsonBuilder, JsonArray, JsonObject, JsonParser}
import org.slf4j.LoggerFactory
import scala.util.{Try, Success, Failure}

/**
 * HTTP Client for calling the Python Inference Service.
 * Implements:
 * - Batch HTTP calls (NOT row-by-row)
 * - Exponential backoff retry
 * - Timeout handling
 * - Latency logging
 */
object InferenceClient {

  private val logger = LoggerFactory.getLogger(this.getClass)
  private val gson: Gson = new GsonBuilder().create()

  private val MAX_RETRIES = 3
  private val INITIAL_BACKOFF_MS = 1000
  private val CONNECTION_TIMEOUT_MS = 10000
  private val READ_TIMEOUT_MS = 60000

  /**
   * Send a batch of items to the inference service.
   * Returns a list of (commentId, generatedReply, latencyMs) tuples.
   */
  def batchInfer(
    inferenceUrl: String,
    items: Seq[(String, String, String, String)] // (commentId, postText, commentText, imageCaption)
  ): Seq[(String, String, Long)] = {

    if (items.isEmpty) return Seq.empty

    logger.info(s"Sending batch inference request with ${items.size} items to $inferenceUrl")
    val startTime = System.currentTimeMillis()

    val requestBody = buildRequestJson(items)
    
    val result = retryWithBackoff(MAX_RETRIES, INITIAL_BACKOFF_MS) {
      val response = Http(s"$inferenceUrl/generate")
        .postData(requestBody)
        .header("Content-Type", "application/json")
        .option(HttpOptions.connTimeout(CONNECTION_TIMEOUT_MS))
        .option(HttpOptions.readTimeout(READ_TIMEOUT_MS))
        .asString

      if (response.isSuccess) {
        parseResponseJson(response.body)
      } else {
        throw new RuntimeException(s"Inference service returned ${response.code}: ${response.body}")
      }
    }

    val totalLatency = System.currentTimeMillis() - startTime
    logger.info(s"Batch inference completed in ${totalLatency}ms for ${items.size} items")

    result.getOrElse {
      logger.error("All retry attempts failed for batch inference")
      items.map { case (id, _, _, _) =>
        (id, s"[Error: Inference failed after $MAX_RETRIES retries]", -1L)
      }
    }
  }

  private def buildRequestJson(items: Seq[(String, String, String, String)]): String = {
    val jsonObj = new JsonObject()
    val jsonItems = new JsonArray()

    items.foreach { case (commentId, postText, commentText, imageCaption) =>
      val item = new JsonObject()
      item.addProperty("id", commentId)
      item.addProperty("post_text", postText)
      item.addProperty("comment_text", commentText)
      item.addProperty("image_caption", imageCaption)
      jsonItems.add(item)
    }

    jsonObj.add("items", jsonItems)
    gson.toJson(jsonObj)
  }

  private def parseResponseJson(responseBody: String): Seq[(String, String, Long)] = {
    try {
      val jsonObj = new JsonParser().parse(responseBody).getAsJsonObject
      val replies = jsonObj.getAsJsonArray("replies")
      
      val result = scala.collection.mutable.ArrayBuffer[(String, String, Long)]()
      val iter = replies.iterator()
      while (iter.hasNext) {
        val reply = iter.next().getAsJsonObject
        val id = reply.get("id").getAsString
        val generatedReply = reply.get("generated_reply").getAsString
        val latencyMs = if (reply.has("latency_ms")) reply.get("latency_ms").getAsLong else 0L
        result += ((id, generatedReply, latencyMs))
      }
      result.toSeq
    } catch {
      case e: Exception =>
        logger.error(s"Failed to parse inference response: ${e.getMessage}")
        Seq.empty
    }
  }

  /**
   * Retry with exponential backoff.
   */
  private def retryWithBackoff[T](maxRetries: Int, initialBackoffMs: Long)(fn: => T): Option[T] = {
    var attempt = 0
    var backoffMs = initialBackoffMs

    while (attempt < maxRetries) {
      Try(fn) match {
        case Success(result) => return Some(result)
        case Failure(e) =>
          attempt += 1
          if (attempt < maxRetries) {
            logger.warn(s"Inference attempt $attempt failed: ${e.getMessage}. Retrying in ${backoffMs}ms...")
            Thread.sleep(backoffMs)
            backoffMs *= 2
          } else {
            logger.error(s"All $maxRetries inference attempts failed: ${e.getMessage}")
          }
      }
    }
    None
  }
}
