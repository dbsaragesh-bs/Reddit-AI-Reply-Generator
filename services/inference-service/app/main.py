"""
Inference Service - Python
Handles LLM inference for reply generation.

Responsibilities:
- Accept batched inputs
- Call pretrained LLM (Groq/OpenAI API or mock)
- Return structured replies
- Multi-comment batching: send up to N comments per LLM call
- Async processing
- Exponential backoff retry with 429 handling
- Rate limiting
- Latency logging

NO preprocessing logic here.
"""

import os
import json
import time
import uuid
import asyncio
import re
from typing import List, Optional, Dict, Tuple

import httpx
import structlog
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

# Structured logging
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.processors.JSONRenderer(),
    ]
)
logger = structlog.get_logger()

app = FastAPI(
    title="Reddit Reply Inference Service",
    description="LLM-powered reply generation for social media comments",
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuration
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "mock")
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_API_KEYS = os.getenv(
    "GROQ_API_KEYS", ""
)  # Comma-separated list for parallel inference
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT_REQUESTS", "5"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "60"))
# How many comments to pack into a single LLM call
COMMENTS_PER_LLM_CALL = int(os.getenv("COMMENTS_PER_LLM_CALL", "10"))
# Rate limiting: max requests per minute per key (Groq free = 30 RPM)
RATE_LIMIT_RPM = int(os.getenv("RATE_LIMIT_RPM", "28"))

# Build the list of all available Groq API keys
_groq_key_list: List[str] = []
if GROQ_API_KEYS:
    _groq_key_list = [k.strip() for k in GROQ_API_KEYS.split(",") if k.strip()]
if GROQ_API_KEY and GROQ_API_KEY not in _groq_key_list:
    _groq_key_list.append(GROQ_API_KEY)

# Semaphore for concurrency control — scale with number of keys
_effective_concurrent = (
    max(MAX_CONCURRENT, len(_groq_key_list) * 3) if _groq_key_list else MAX_CONCURRENT
)
semaphore = asyncio.Semaphore(_effective_concurrent)


# Token-bucket style rate limiter
class RateLimiter:
    """Simple sliding-window rate limiter for API calls."""

    def __init__(self, max_rpm: int):
        self.max_rpm = max_rpm
        self._timestamps: list = []
        self._lock = asyncio.Lock()

    async def acquire(self):
        """Wait until we can make a request without exceeding RPM."""
        async with self._lock:
            now = time.time()
            self._timestamps = [t for t in self._timestamps if now - t < 60.0]

            if len(self._timestamps) >= self.max_rpm:
                wait_time = 60.0 - (now - self._timestamps[0]) + 0.1
                if wait_time > 0:
                    logger.info(
                        f"Rate limit: waiting {wait_time:.1f}s ({len(self._timestamps)} reqs in last 60s)"
                    )
                    await asyncio.sleep(wait_time)
                    now = time.time()
                    self._timestamps = [t for t in self._timestamps if now - t < 60.0]

            self._timestamps.append(time.time())


rate_limiter = RateLimiter(RATE_LIMIT_RPM)  # Legacy: used by single-key fallback paths


# ============================================================
# Pydantic Models
# ============================================================
class InferenceItem(BaseModel):
    id: str
    post_text: str
    comment_text: str
    image_caption: str = ""


class BatchInferenceRequest(BaseModel):
    items: List[InferenceItem]


class InferenceReply(BaseModel):
    id: str
    generated_reply: str
    latency_ms: int


class BatchInferenceResponse(BaseModel):
    replies: List[InferenceReply]


class SingleInferenceRequest(BaseModel):
    post_text: str
    comments: List[str] = []
    image_caption: str = ""


class SingleInferenceResponse(BaseModel):
    generated_reply: str


class DescribeImageRequest(BaseModel):
    image_url: str
    image_base64: str = ""
    context: str = ""


class DescribeImageResponse(BaseModel):
    caption: str
    latency_ms: int


# ============================================================
# LLM Providers (with multi-comment support)
# ============================================================
class MockLLMProvider:
    """Mock LLM provider for testing without API keys."""

    async def generate_multi(
        self, post_text: str, comments: List[Tuple[str, str]], image_caption: str
    ) -> Dict[str, str]:
        """Generate replies for multiple comments on the same post in one call."""
        await asyncio.sleep(0.05)
        import random

        templates = [
            "interesting take, hadn't thought of it that way",
            "yeah this is pretty much spot on",
            "hard disagree but i see where you're coming from",
            "we had the same issue at my company, ended up going a different route",
            "solid point, especially the part about scalability",
            "lol this is exactly what happened to us last sprint",
            "can confirm, been running this setup for 6 months",
            "nah this ain't it chief",
        ]
        result = {}
        for comment_id, _ in comments:
            result[comment_id] = random.choice(templates)
        return result

    async def generate(
        self, post_text: str, comment_text: str, image_caption: str
    ) -> str:
        """Fallback single-comment generate for backward compat."""
        result = await self.generate_multi(
            post_text, [("single", comment_text)], image_caption
        )
        return result["single"]

    async def generate_mixed_batch(
        self, items: List[Tuple[str, str, str, str, str]]
    ) -> Dict[str, str]:
        """Mock mixed-batch: generate random replies for cross-post items."""
        await asyncio.sleep(0.05)
        import random

        templates = [
            "interesting take, hadn't thought of it that way",
            "yeah this is pretty much spot on",
            "hard disagree but i see where you're coming from",
            "we had the same issue at my company, ended up going a different route",
            "solid point, especially the part about scalability",
            "lol this is exactly what happened to us last sprint",
            "can confirm, been running this setup for 6 months",
            "nah this ain't it chief",
        ]
        result = {}
        for cid, _, _, _, _ in items:
            result[cid] = random.choice(templates)
        return result


class GroqKeySlot:
    """One Groq API key with its own rate limiter."""

    def __init__(self, api_key: str, slot_id: int, rpm: int):
        self.api_key = api_key
        self.slot_id = slot_id
        self.rate_limiter = RateLimiter(rpm)
        self.base_url = "https://api.groq.com/openai/v1/chat/completions"
        self.model = "llama-3.1-8b-instant"
        self._call_count = 0
        self._error_count = 0
        self._lock = asyncio.Lock()

    async def call_llm(self, messages: list, max_tokens: int) -> dict:
        """Make a single LLM call using this key's rate limiter."""
        await self.rate_limiter.acquire()
        async with self._lock:
            self._call_count += 1

        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
            response = await client.post(
                self.base_url,
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": self.model,
                    "messages": messages,
                    "max_tokens": max_tokens,
                    "temperature": 0.9,
                },
            )

            if response.status_code == 429:
                async with self._lock:
                    self._error_count += 1
                retry_after = float(response.headers.get("retry-after", "5"))
                raise RateLimitError(retry_after, self.slot_id)

            response.raise_for_status()
            return response.json()

    @property
    def stats(self) -> dict:
        return {
            "slot_id": self.slot_id,
            "calls": self._call_count,
            "errors": self._error_count,
            "key_prefix": self.api_key[:12] + "...",
        }


class RateLimitError(Exception):
    """Raised when a Groq key hits 429."""

    def __init__(self, retry_after: float, slot_id: int):
        self.retry_after = retry_after
        self.slot_id = slot_id
        super().__init__(
            f"Key slot {slot_id}: rate limited, retry after {retry_after}s"
        )


class GroqKeyPool:
    """Round-robin pool of Groq API keys for parallel inference."""

    def __init__(self, keys: List[str], rpm_per_key: int):
        self.slots = [GroqKeySlot(k, i, rpm_per_key) for i, k in enumerate(keys)]
        self._idx = 0
        self._lock = asyncio.Lock()
        logger.info(
            f"Groq key pool initialized: {len(self.slots)} keys, "
            f"{rpm_per_key} RPM each = {len(self.slots) * rpm_per_key} RPM total"
        )

    async def next_slot(self) -> GroqKeySlot:
        """Get the next key slot (round-robin)."""
        async with self._lock:
            slot = self.slots[self._idx % len(self.slots)]
            self._idx += 1
            return slot

    def stats(self) -> list:
        return [s.stats for s in self.slots]


class GroqLLMProvider:
    """Groq API provider with multi-key parallel inference."""

    def __init__(self, keys: List[str], rpm_per_key: int):
        self.pool = GroqKeyPool(keys, rpm_per_key)
        self.num_keys = len(keys)

    async def _call_with_retry(self, messages: list, max_tokens: int) -> dict:
        """Call LLM with automatic retry on rate limit, rotating to next key."""
        max_retries = 5
        for attempt in range(max_retries):
            slot = await self.pool.next_slot()
            try:
                return await slot.call_llm(messages, max_tokens)
            except RateLimitError as e:
                logger.warn(
                    f"Key slot {e.slot_id} rate limited, rotating (attempt {attempt + 1})"
                )
                await asyncio.sleep(min(e.retry_after, 3.0))
                continue
            except httpx.TimeoutException:
                logger.warn(
                    f"Groq timeout on slot {slot.slot_id} (attempt {attempt + 1})"
                )
                await asyncio.sleep(2**attempt)
                continue
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:
                    retry_after = float(e.response.headers.get("retry-after", "5"))
                    await asyncio.sleep(min(retry_after, 3.0))
                    continue
                raise
        raise RuntimeError(f"Failed after {max_retries} retries across key pool")

    async def generate_multi(
        self, post_text: str, comments: List[Tuple[str, str]], image_caption: str
    ) -> Dict[str, str]:
        """Generate replies for multiple comments on the same post in one LLM call."""
        prompt = self._build_multi_prompt(post_text, comments, image_caption)
        max_tokens = min(80 * len(comments) + 50, 1024)

        messages = [
            {
                "role": "system",
                "content": (
                    "You are a real Reddit user. Write like an actual person -- casual, short, and direct. "
                    "Use lowercase, slang, and internet speak when it fits. No filler phrases. Never sound like an AI. "
                    "One or two sentences max per reply. Match the vibe of the conversation.\n\n"
                    "IMPORTANT: You will be given a post and multiple comments. Reply to EACH comment separately. "
                    "Format your response EXACTLY as:\n"
                    "[1] your reply to comment 1\n"
                    "[2] your reply to comment 2\n"
                    "...and so on. Each reply must start with [number] on its own line."
                ),
            },
            {"role": "user", "content": prompt},
        ]

        data = await self._call_with_retry(messages, max_tokens)
        raw_text = data["choices"][0]["message"]["content"]
        return self._parse_multi_response(raw_text, comments)

    async def generate(
        self, post_text: str, comment_text: str, image_caption: str
    ) -> str:
        """Single-comment generate (used by streaming path)."""
        result = await self.generate_multi(
            post_text, [("single", comment_text)], image_caption
        )
        return result.get("single", "[Error: no reply generated]")

    async def generate_mixed_batch(
        self, items: List[Tuple[str, str, str, str, str]]
    ) -> Dict[str, str]:
        """Generate replies for comments from DIFFERENT posts in one LLM call."""
        prompt = self._build_mixed_prompt(items)
        max_tokens = min(80 * len(items) + 100, 2048)

        messages = [
            {
                "role": "system",
                "content": (
                    "You are a real Reddit user. Write like an actual person -- casual, short, and direct. "
                    "Use lowercase, slang, and internet speak when it fits. No filler phrases. Never sound like an AI. "
                    "One or two sentences max per reply. Match the vibe of the conversation.\n\n"
                    "IMPORTANT: You will be given multiple post+comment pairs. Reply to EACH comment separately. "
                    "Format your response EXACTLY as:\n"
                    "[1] your reply to item 1\n"
                    "[2] your reply to item 2\n"
                    "...and so on. Each reply must start with [number] on its own line."
                ),
            },
            {"role": "user", "content": prompt},
        ]

        data = await self._call_with_retry(messages, max_tokens)
        raw_text = data["choices"][0]["message"]["content"]
        return self._parse_mixed_response(raw_text, items)

    def _build_multi_prompt(
        self,
        post_text: str,
        comments: List[Tuple[str, str]],
        image_caption: str,
    ) -> str:
        prompt = f"Post: {post_text}"
        if image_caption:
            prompt += f"\n(image: {image_caption})"
        prompt += "\n\nComments to reply to:"
        for i, (cid, text) in enumerate(comments, 1):
            prompt += f"\n[{i}] {text}"
        prompt += "\n\nReply to each comment above (use [1], [2], etc.):"
        return prompt

    def _parse_multi_response(
        self, raw_text: str, comments: List[Tuple[str, str]]
    ) -> Dict[str, str]:
        """Parse the LLM response into a dict of {comment_id: reply}."""
        result = {}

        pattern = r"\[(\d+)\]\s*(.*?)(?=\[\d+\]|$)"
        matches = re.findall(pattern, raw_text, re.DOTALL)

        if matches:
            for num_str, reply_text in matches:
                idx = int(num_str) - 1
                if 0 <= idx < len(comments):
                    comment_id = comments[idx][0]
                    result[comment_id] = reply_text.strip()

        if len(result) < len(comments):
            lines = [l.strip() for l in raw_text.strip().split("\n") if l.strip()]
            cleaned_lines = []
            for line in lines:
                cleaned = re.sub(r"^\[\d+\]\s*", "", line).strip()
                if cleaned:
                    cleaned_lines.append(cleaned)

            for i, (comment_id, _) in enumerate(comments):
                if comment_id not in result:
                    if i < len(cleaned_lines):
                        result[comment_id] = cleaned_lines[i]
                    else:
                        result[comment_id] = raw_text.strip()[:150]

        for comment_id, _ in comments:
            if comment_id not in result:
                result[comment_id] = (
                    raw_text.strip()[:150]
                    if raw_text.strip()
                    else "[no reply generated]"
                )

        return result

    def _build_mixed_prompt(self, items: List[Tuple[str, str, str, str, str]]) -> str:
        """Build a prompt with multiple post+comment pairs."""
        prompt = "Reply to each of the following post+comment pairs:\n"
        for i, (cid, post_text, comment_text, image_caption, _) in enumerate(items, 1):
            prompt += f"\n[{i}] Post: {post_text[:200]}"
            if image_caption:
                prompt += f" (image: {image_caption[:100]})"
            prompt += f"\n    Comment: {comment_text[:300]}"
        prompt += "\n\nReply to each item above (use [1], [2], etc.):"
        return prompt

    def _parse_mixed_response(
        self, raw_text: str, items: List[Tuple[str, str, str, str, str]]
    ) -> Dict[str, str]:
        """Parse mixed-batch LLM response into {comment_id: reply}."""
        result = {}
        pattern = r"\[(\d+)\]\s*(.*?)(?=\[\d+\]|$)"
        matches = re.findall(pattern, raw_text, re.DOTALL)

        if matches:
            for num_str, reply_text in matches:
                idx = int(num_str) - 1
                if 0 <= idx < len(items):
                    comment_id = items[idx][0]
                    result[comment_id] = reply_text.strip()

        if len(result) < len(items):
            lines = [l.strip() for l in raw_text.strip().split("\n") if l.strip()]
            cleaned_lines = [
                re.sub(r"^\[\d+\]\s*", "", l).strip()
                for l in lines
                if re.sub(r"^\[\d+\]\s*", "", l).strip()
            ]
            for i, (cid, _, _, _, _) in enumerate(items):
                if cid not in result:
                    if i < len(cleaned_lines):
                        result[cid] = cleaned_lines[i]
                    else:
                        result[cid] = (
                            raw_text.strip()[:150]
                            if raw_text.strip()
                            else "[no reply generated]"
                        )

        for cid, _, _, _, _ in items:
            if cid not in result:
                result[cid] = (
                    raw_text.strip()[:150]
                    if raw_text.strip()
                    else "[no reply generated]"
                )

        return result


class OpenAILLMProvider:
    """OpenAI API provider with multi-comment batching."""

    def __init__(self):
        self.api_key = OPENAI_API_KEY
        self.base_url = "https://api.openai.com/v1/chat/completions"
        self.model = "gpt-3.5-turbo"

    async def generate_multi(
        self, post_text: str, comments: List[Tuple[str, str]], image_caption: str
    ) -> Dict[str, str]:
        """Generate replies for multiple comments in a single call."""
        prompt = f"Post: {post_text}"
        if image_caption:
            prompt += f"\n(image: {image_caption})"
        prompt += "\n\nComments to reply to:"
        for i, (cid, text) in enumerate(comments, 1):
            prompt += f"\n[{i}] {text}"
        prompt += "\n\nReply to each comment above (use [1], [2], etc.):"

        max_tokens = min(80 * len(comments) + 50, 1024)

        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
            response = await client.post(
                self.base_url,
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": self.model,
                    "messages": [
                        {
                            "role": "system",
                            "content": (
                                "You are a real Reddit user. Write like an actual person -- casual, short, and direct. "
                                "Use lowercase, slang, and internet speak when it fits. No filler phrases. Never sound like an AI. "
                                "One or two sentences max per reply. Match the vibe of the conversation.\n\n"
                                "IMPORTANT: Reply to EACH comment separately. Format:\n"
                                "[1] your reply to comment 1\n"
                                "[2] your reply to comment 2\n"
                            ),
                        },
                        {"role": "user", "content": prompt},
                    ],
                    "max_tokens": max_tokens,
                    "temperature": 0.9,
                },
            )
            response.raise_for_status()
            data = response.json()
            raw_text = data["choices"][0]["message"]["content"]

            # Reuse Groq's parser (create temporary instance for parsing only)
            parser = GroqLLMProvider(keys=["dummy"], rpm_per_key=30)
            return parser._parse_multi_response(raw_text, comments)

    async def generate(
        self, post_text: str, comment_text: str, image_caption: str
    ) -> str:
        result = await self.generate_multi(
            post_text, [("single", comment_text)], image_caption
        )
        return result.get("single", "[Error]")

    async def generate_mixed_batch(
        self, items: List[Tuple[str, str, str, str, str]]
    ) -> Dict[str, str]:
        """Generate replies for comments from different posts in a single OpenAI call."""
        parser = GroqLLMProvider(keys=["dummy"], rpm_per_key=30)
        prompt = parser._build_mixed_prompt(items)
        max_tokens = min(80 * len(items) + 100, 2048)

        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
            response = await client.post(
                self.base_url,
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": self.model,
                    "messages": [
                        {
                            "role": "system",
                            "content": (
                                "You are a real Reddit user. Write like an actual person -- casual, short, and direct. "
                                "Use lowercase, slang, and internet speak when it fits. No filler phrases. Never sound like an AI. "
                                "One or two sentences max per reply. Match the vibe of the conversation.\n\n"
                                "IMPORTANT: You will be given multiple post+comment pairs. Reply to EACH comment separately. "
                                "Format your response EXACTLY as:\n"
                                "[1] your reply to item 1\n"
                                "[2] your reply to item 2\n"
                            ),
                        },
                        {"role": "user", "content": prompt},
                    ],
                    "max_tokens": max_tokens,
                    "temperature": 0.9,
                },
            )
            response.raise_for_status()
            data = response.json()
            raw_text = data["choices"][0]["message"]["content"]
            return parser._parse_mixed_response(raw_text, items)


# Provider factory
def get_provider():
    if LLM_PROVIDER == "groq" and _groq_key_list:
        logger.info(
            "Using Groq LLM provider with key pool",
            model="llama-3.1-8b-instant",
            num_keys=len(_groq_key_list),
            rpm_per_key=RATE_LIMIT_RPM,
            total_rpm=len(_groq_key_list) * RATE_LIMIT_RPM,
        )
        return GroqLLMProvider(keys=_groq_key_list, rpm_per_key=RATE_LIMIT_RPM)
    elif LLM_PROVIDER == "groq" and GROQ_API_KEY:
        logger.info(
            "Using Groq LLM provider (single key)", model="llama-3.1-8b-instant"
        )
        return GroqLLMProvider(keys=[GROQ_API_KEY], rpm_per_key=RATE_LIMIT_RPM)
    elif LLM_PROVIDER == "openai" and OPENAI_API_KEY:
        logger.info("Using OpenAI LLM provider", model="gpt-3.5-turbo")
        return OpenAILLMProvider()
    else:
        logger.info("Using Mock LLM provider (no API key configured)")
        return MockLLMProvider()


provider = get_provider()


# ============================================================
# Endpoints
# ============================================================
@app.get("/health")
async def health_check():
    num_keys = getattr(provider, "num_keys", 1)
    return {
        "status": "healthy",
        "service": "inference-service",
        "provider": LLM_PROVIDER,
        "num_api_keys": num_keys,
        "total_rpm": num_keys * RATE_LIMIT_RPM,
        "max_concurrent": _effective_concurrent,
        "comments_per_llm_call": COMMENTS_PER_LLM_CALL,
    }


@app.post("/generate", response_model=BatchInferenceResponse)
async def generate_batch(request: BatchInferenceRequest):
    """
    Batch inference endpoint.
    Groups items by post_text, sends up to COMMENTS_PER_LLM_CALL comments
    per single LLM call, dramatically reducing API calls needed.

    Example: 50 items with 10 unique posts, 5 comments each = 10 LLM calls
    instead of 50. At 28 RPM that's ~21s instead of ~107s.
    """
    start_time = time.time()
    batch_count = len(request.items)
    logger.info("Batch inference request received", count=batch_count)

    if not request.items:
        return BatchInferenceResponse(replies=[])

    # Group items by post_text (comments on the same post go together)
    post_groups: Dict[str, List[InferenceItem]] = {}
    for item in request.items:
        key = item.post_text[:200]  # Group by post text (truncated for key)
        if key not in post_groups:
            post_groups[key] = []
        post_groups[key].append(item)

    logger.info(
        f"Grouped {batch_count} items into {len(post_groups)} post groups "
        f"(avg {batch_count / max(len(post_groups), 1):.1f} comments/post)"
    )

    # Build LLM call tasks
    # For same-post groups with multiple comments: use generate_multi (same-post batching)
    # For singleton groups (1 comment per post): pack into mixed-post batches
    all_tasks = []  # Each task is (task_type, items)
    singleton_items = []  # Accumulate singletons for cross-post batching

    for post_key, items in post_groups.items():
        if len(items) >= 2:
            # Multi-comment same-post group: chunk by COMMENTS_PER_LLM_CALL
            for chunk_start in range(0, len(items), COMMENTS_PER_LLM_CALL):
                chunk = items[chunk_start : chunk_start + COMMENTS_PER_LLM_CALL]
                all_tasks.append(("same_post", chunk))
        else:
            # Single comment for this post: accumulate for cross-post batching
            singleton_items.extend(items)

    # Pack singletons into mixed-post batches of COMMENTS_PER_LLM_CALL
    has_mixed_batch = hasattr(provider, "generate_mixed_batch")
    if singleton_items and has_mixed_batch:
        for chunk_start in range(0, len(singleton_items), COMMENTS_PER_LLM_CALL):
            chunk = singleton_items[chunk_start : chunk_start + COMMENTS_PER_LLM_CALL]
            all_tasks.append(("mixed_post", chunk))
    elif singleton_items:
        # Fallback: treat each singleton as a same-post task
        for item in singleton_items:
            all_tasks.append(("same_post", [item]))

    total_llm_calls = len(all_tasks)
    logger.info(
        f"Will make {total_llm_calls} LLM calls for {batch_count} comments "
        f"({batch_count / max(total_llm_calls, 1):.1f}x reduction, "
        f"{len(singleton_items)} in cross-post batches)"
    )

    # Process LLM call tasks with concurrency control
    result_replies = []
    # Scale concurrency with number of API keys — each key can handle independent calls
    num_keys = getattr(provider, "num_keys", 1)
    concurrent_limit = max(num_keys, min(MAX_CONCURRENT, 3))

    logger.info(
        f"Processing {total_llm_calls} LLM tasks with concurrency={concurrent_limit} "
        f"({num_keys} API keys available)"
    )

    for i in range(0, len(all_tasks), concurrent_limit):
        task_chunk = all_tasks[i : i + concurrent_limit]
        coroutines = []
        for task_type, task_items in task_chunk:
            if task_type == "mixed_post":
                coroutines.append(process_mixed_post_task(task_items))
            else:
                coroutines.append(process_multi_comment_task(task_items))
        results = await asyncio.gather(*coroutines, return_exceptions=True)

        for j, res in enumerate(results):
            if isinstance(res, Exception):
                logger.error(f"Multi-comment task failed: {res}")
                # Return error replies for all items in this task
                _, task_items = task_chunk[j]
                for item in task_items:
                    result_replies.append(
                        InferenceReply(
                            id=item.id,
                            generated_reply=f"[Error: {str(res)[:80]}]",
                            latency_ms=-1,
                        )
                    )
            elif isinstance(res, list):
                result_replies.extend(res)

        done = min(i + concurrent_limit, len(all_tasks))
        comments_done = sum(len(t[1]) for t in all_tasks[:done])
        logger.info(
            f"Progress: {done}/{total_llm_calls} LLM calls done "
            f"({comments_done}/{batch_count} comments)"
        )

    total_latency = int((time.time() - start_time) * 1000)
    logger.info(
        "Batch inference complete",
        count=len(result_replies),
        llm_calls=total_llm_calls,
        total_latency_ms=total_latency,
        avg_per_comment_ms=total_latency // max(len(result_replies), 1),
    )

    return BatchInferenceResponse(replies=result_replies)


async def process_multi_comment_task(
    items: List[InferenceItem],
) -> List[InferenceReply]:
    """Process multiple comments for the same post in one LLM call."""
    async with semaphore:
        start = time.time()
        post_text = items[0].post_text
        image_caption = items[0].image_caption

        # Build (comment_id, comment_text) list
        comments = [(item.id, item.comment_text) for item in items]

        try:
            reply_map = await asyncio.wait_for(
                provider.generate_multi(post_text, comments, image_caption),
                timeout=REQUEST_TIMEOUT,
            )
            latency_ms = int((time.time() - start) * 1000)
            per_comment_ms = latency_ms // max(len(items), 1)

            logger.info(
                f"Multi-comment inference done: {len(items)} comments in {latency_ms}ms "
                f"({per_comment_ms}ms/comment)"
            )

            result = []
            for item in items:
                reply_text = reply_map.get(
                    item.id, "[Error: reply not found in LLM output]"
                )
                result.append(
                    InferenceReply(
                        id=item.id,
                        generated_reply=reply_text,
                        latency_ms=per_comment_ms,
                    )
                )
            return result

        except asyncio.TimeoutError:
            latency_ms = int((time.time() - start) * 1000)
            logger.error(f"Multi-comment inference timeout after {latency_ms}ms")
            return [
                InferenceReply(
                    id=item.id,
                    generated_reply="[Reply generation timed out]",
                    latency_ms=latency_ms,
                )
                for item in items
            ]


async def process_mixed_post_task(
    items: List[InferenceItem],
) -> List[InferenceReply]:
    """Process comments from DIFFERENT posts in one LLM call (cross-post batching)."""
    async with semaphore:
        start = time.time()

        # Build tuples: (comment_id, post_text, comment_text, image_caption, "")
        items_tuples = [
            (item.id, item.post_text, item.comment_text, item.image_caption, "")
            for item in items
        ]

        try:
            reply_map = await asyncio.wait_for(
                provider.generate_mixed_batch(items_tuples),
                timeout=REQUEST_TIMEOUT,
            )
            latency_ms = int((time.time() - start) * 1000)
            per_comment_ms = latency_ms // max(len(items), 1)

            logger.info(
                f"Mixed-post inference done: {len(items)} comments from different posts "
                f"in {latency_ms}ms ({per_comment_ms}ms/comment)"
            )

            result = []
            for item in items:
                reply_text = reply_map.get(
                    item.id, "[Error: reply not found in LLM output]"
                )
                result.append(
                    InferenceReply(
                        id=item.id,
                        generated_reply=reply_text,
                        latency_ms=per_comment_ms,
                    )
                )
            return result

        except asyncio.TimeoutError:
            latency_ms = int((time.time() - start) * 1000)
            logger.error(f"Mixed-post inference timeout after {latency_ms}ms")
            return [
                InferenceReply(
                    id=item.id,
                    generated_reply="[Reply generation timed out]",
                    latency_ms=latency_ms,
                )
                for item in items
            ]


async def process_single_item(item: InferenceItem) -> InferenceReply:
    """Process a single inference item (backward compat, used by streaming)."""
    async with semaphore:
        start = time.time()
        try:
            reply_text = await asyncio.wait_for(
                provider.generate(
                    item.post_text, item.comment_text, item.image_caption
                ),
                timeout=REQUEST_TIMEOUT,
            )
            latency_ms = int((time.time() - start) * 1000)
            return InferenceReply(
                id=item.id, generated_reply=reply_text, latency_ms=latency_ms
            )
        except asyncio.TimeoutError:
            latency_ms = int((time.time() - start) * 1000)
            return InferenceReply(
                id=item.id,
                generated_reply="[Reply generation timed out]",
                latency_ms=latency_ms,
            )


@app.post("/generate-single", response_model=SingleInferenceResponse)
async def generate_single(request: SingleInferenceRequest):
    """Single inference for real-time comments via streaming pipeline."""
    start = time.time()
    comment_text = (
        " | ".join(request.comments) if request.comments else "General comment"
    )
    reply = await provider.generate(
        request.post_text, comment_text, request.image_caption
    )
    latency = int((time.time() - start) * 1000)
    logger.info("Single inference complete", latency_ms=latency)
    return SingleInferenceResponse(generated_reply=reply)


@app.get("/metrics")
async def get_metrics():
    """Return basic service metrics."""
    num_keys = getattr(provider, "num_keys", 1)
    key_stats = provider.pool.stats() if hasattr(provider, "pool") else []
    return {
        "service": "inference-service",
        "provider": LLM_PROVIDER,
        "num_api_keys": num_keys,
        "total_rpm": num_keys * RATE_LIMIT_RPM,
        "max_concurrent": _effective_concurrent,
        "comments_per_llm_call": COMMENTS_PER_LLM_CALL,
        "rate_limit_rpm_per_key": RATE_LIMIT_RPM,
        "timeout_seconds": REQUEST_TIMEOUT,
        "key_stats": key_stats,
    }


@app.post("/describe-image", response_model=DescribeImageResponse)
async def describe_image(request: DescribeImageRequest):
    """
    Use a vision-capable LLM to describe an image.
    Uses meta-llama/llama-4-scout-17b-16e-instruct on Groq for multimodal analysis.
    """
    start = time.time()
    logger.info("Image description request", image_url=request.image_url[:80])

    if LLM_PROVIDER == "groq" and _groq_key_list:
        try:
            if request.image_base64:
                image_ref = request.image_base64
            else:
                image_ref = request.image_url
            caption = await _describe_image_groq(image_ref, request.context)
        except Exception as e:
            error_detail = str(e)
            if hasattr(e, "__cause__") and hasattr(e.__cause__, "response"):
                try:
                    error_detail = e.__cause__.response.text
                except Exception:
                    pass
            elif hasattr(e, "response"):
                try:
                    error_detail = e.response.text
                except Exception:
                    pass
            logger.error("Vision model failed, using fallback", error=error_detail)
            caption = "[Image attached to post]"
    else:
        caption = "[Image attached to post]"

    latency_ms = int((time.time() - start) * 1000)
    logger.info(
        "Image description complete", latency_ms=latency_ms, caption_len=len(caption)
    )
    return DescribeImageResponse(caption=caption, latency_ms=latency_ms)


@retry(
    stop=stop_after_attempt(2),
    wait=wait_exponential(multiplier=1, min=1, max=5),
    retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.TimeoutException)),
)
async def _describe_image_groq(image_ref: str, context: str = "") -> str:
    """Call Groq vision model to describe an image."""
    system_prompt = (
        "You are an image analysis assistant. Describe the image in detail, focusing on "
        "the main subject, key visual elements, text visible, emotions, and context. "
        "Keep the description concise but informative (2-4 sentences)."
    )
    user_content = []
    if context:
        user_content.append(
            {
                "type": "text",
                "text": f"Context for this image (post title/text): {context}\n\nDescribe this image:",
            }
        )
    else:
        user_content.append({"type": "text", "text": "Describe this image:"})
    user_content.append({"type": "image_url", "image_url": {"url": image_ref}})

    # Pick a key from pool for vision calls
    vision_api_key = _groq_key_list[0] if _groq_key_list else GROQ_API_KEY

    async with httpx.AsyncClient(timeout=60) as client:
        response = await client.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {vision_api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": "meta-llama/llama-4-scout-17b-16e-instruct",
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_content},
                ],
                "max_tokens": 200,
                "temperature": 0.3,
            },
        )
        if response.status_code != 200:
            logger.error(
                "Groq vision API error",
                status=response.status_code,
                body=response.text[:500],
            )
        response.raise_for_status()
        data = response.json()
        return data["choices"][0]["message"]["content"]


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
