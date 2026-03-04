package com.reddit.batch

import scalaj.http.{Http, HttpOptions}
import com.google.gson.{Gson, GsonBuilder, JsonArray, JsonObject, JsonParser}
import org.slf4j.LoggerFactory
import scala.util.{Try, Success, Failure}

/**
 * HTTP Client for batch inference calls.
 * Same exponential backoff + batch pattern as streaming service.
 */
object BatchInferenceClient {

  private val logger = LoggerFactory.getLogger(this.getClass)
  private val gson: Gson = new GsonBuilder().create()

  private val MAX_RETRIES = 3
  private val INITIAL_BACKOFF_MS = 1000
  private val CONNECTION_TIMEOUT_MS = 10000
  private val READ_TIMEOUT_MS = 120000 // Higher timeout for batch

  /**
   * Send a batch of items to inference service.
   * Returns list of (commentId, generatedReply, latencyMs).
   */
  def batchInfer(
    inferenceUrl: String,
    items: Seq[(String, String, String, String)] // (commentId, postText, commentText, imageCaption)
  ): Seq[(String, String, Long)] = {

    if (items.isEmpty) return Seq.empty

    logger.info(s"Batch inference request: ${items.size} items → $inferenceUrl")
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
        throw new RuntimeException(s"Inference error ${response.code}: ${response.body}")
      }
    }

    val totalLatency = System.currentTimeMillis() - startTime
    logger.info(s"Batch inference completed: ${items.size} items in ${totalLatency}ms")

    result.getOrElse {
      logger.error("All retries exhausted for batch inference")
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

  private def parseResponseJson(body: String): Seq[(String, String, Long)] = {
    try {
      val jsonObj = new JsonParser().parse(body).getAsJsonObject
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
        logger.error(s"Failed to parse response: ${e.getMessage}")
        Seq.empty
    }
  }

  private def retryWithBackoff[T](maxRetries: Int, initialBackoffMs: Long)(fn: => T): Option[T] = {
    var attempt = 0
    var backoffMs = initialBackoffMs

    while (attempt < maxRetries) {
      Try(fn) match {
        case Success(result) => return Some(result)
        case Failure(e) =>
          attempt += 1
          if (attempt < maxRetries) {
            logger.warn(s"Attempt $attempt failed: ${e.getMessage}. Retry in ${backoffMs}ms...")
            Thread.sleep(backoffMs)
            backoffMs *= 2
          } else {
            logger.error(s"All $maxRetries attempts failed: ${e.getMessage}")
          }
      }
    }
    None
  }
}
