"""
API Gateway Service - Python FastAPI

Responsibilities:
- Simple username-based authentication
- Post creation and listing
- Comment submission (-> Kafka)
- JSON bulk upload (-> HDFS -> Spark Batch)
- WebSocket handling (Kafka replies_topic consumer)
- File download endpoint
- SQLite persistence for all data
- Kafka producer for live events

This is ROUTING ONLY. No preprocessing logic.
"""

import os
import json
import time
import uuid
import asyncio
import sqlite3
from typing import List, Optional, Set
from contextlib import asynccontextmanager

import httpx
import aiofiles
import structlog
from fastapi import (
    FastAPI,
    HTTPException,
    UploadFile,
    File,
    WebSocket,
    WebSocketDisconnect,
    Depends,
    Header,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

# Structured logging
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.processors.JSONRenderer(),
    ]
)
logger = structlog.get_logger()

# Configuration
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
HDFS_URL = os.getenv("HDFS_URL", "http://hadoop-namenode:9870")
HDFS_FS_URL = os.getenv("HDFS_FS_URL", "hdfs://hadoop-namenode:9000")
INFERENCE_URL = os.getenv("INFERENCE_SERVICE_URL", "http://inference-service:8000")
SPARK_BATCH_URL = os.getenv("SPARK_BATCH_URL", "http://spark-batch-service:8085")
CORS_ORIGINS = os.getenv("CORS_ORIGINS", "http://localhost:3000").split(",")
DB_PATH = os.getenv("DATABASE_PATH", "/app/data/app.db")
IMAGES_DIR = os.getenv("IMAGES_DIR", "/app/data/images")

# Kafka producer (initialized in lifespan)
kafka_producer: Optional[AIOKafkaProducer] = None

# WebSocket connections
ws_connections: Set[WebSocket] = set()


# ============================================================
# SQLite Database
# ============================================================
def get_db():
    """Get a SQLite connection."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    """Initialize database tables."""
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            userId TEXT PRIMARY KEY,
            username TEXT UNIQUE NOT NULL,
            createdAt INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS posts (
            postId TEXT PRIMARY KEY,
            userId TEXT NOT NULL,
            username TEXT NOT NULL,
            title TEXT NOT NULL,
            postText TEXT NOT NULL,
            subreddit TEXT DEFAULT 'general',
            imageUrl TEXT DEFAULT '',
            imageCaption TEXT DEFAULT '',
            createdAt INTEGER NOT NULL,
            FOREIGN KEY (userId) REFERENCES users(userId)
        );

        CREATE TABLE IF NOT EXISTS comments (
            commentId TEXT PRIMARY KEY,
            postId TEXT NOT NULL,
            userId TEXT NOT NULL,
            username TEXT NOT NULL,
            commentText TEXT NOT NULL,
            parentCommentId TEXT DEFAULT '',
            createdAt INTEGER NOT NULL,
            FOREIGN KEY (postId) REFERENCES posts(postId),
            FOREIGN KEY (userId) REFERENCES users(userId)
        );

        CREATE TABLE IF NOT EXISTS replies (
            replyId TEXT PRIMARY KEY,
            commentId TEXT NOT NULL,
            postId TEXT NOT NULL,
            generatedReply TEXT NOT NULL,
            timestamp INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS batch_jobs (
            jobId TEXT PRIMARY KEY,
            sparkJobId TEXT DEFAULT '',
            status TEXT NOT NULL,
            inputPath TEXT DEFAULT '',
            outputPath TEXT DEFAULT '',
            uploadedAt INTEGER NOT NULL,
            filename TEXT DEFAULT '',
            totalRecords INTEGER DEFAULT 0,
            processedRecords INTEGER DEFAULT 0
        );

        CREATE INDEX IF NOT EXISTS idx_posts_created ON posts(createdAt DESC);
        CREATE INDEX IF NOT EXISTS idx_comments_post ON comments(postId);
        CREATE INDEX IF NOT EXISTS idx_replies_post ON replies(postId);
        CREATE INDEX IF NOT EXISTS idx_replies_comment ON replies(commentId);
    """)
    conn.close()
    logger.info("Database initialized", path=DB_PATH)


def seed_db():
    """Insert starter posts and comments so the feed isn't empty on first visit."""
    conn = get_db()
    existing = conn.execute("SELECT COUNT(*) FROM posts").fetchone()[0]
    if existing > 0:
        conn.close()
        return

    now = int(time.time() * 1000)

    # ---- seed users ----
    seed_users = [
        {
            "userId": "seed-user-01",
            "username": "TechEnthusiast",
            "createdAt": now - 7200000,
        },
        {
            "userId": "seed-user-02",
            "username": "CosmicExplorer",
            "createdAt": now - 6800000,
        },
        {
            "userId": "seed-user-03",
            "username": "PixelWanderer",
            "createdAt": now - 6400000,
        },
        {
            "userId": "seed-user-04",
            "username": "DataNerd42",
            "createdAt": now - 6000000,
        },
        {
            "userId": "seed-user-05",
            "username": "CuriousCat99",
            "createdAt": now - 5600000,
        },
        {
            "userId": "seed-user-06",
            "username": "NightOwlDev",
            "createdAt": now - 5200000,
        },
    ]
    for u in seed_users:
        conn.execute(
            "INSERT OR IGNORE INTO users (userId, username, createdAt) VALUES (?, ?, ?)",
            (u["userId"], u["username"], u["createdAt"]),
        )

    # ---- seed posts ----
    seed_posts = [
        {
            "postId": "seed-post-01",
            "userId": "seed-user-01",
            "username": "TechEnthusiast",
            "title": "The new Rust compiler is insanely fast",
            "postText": "Just upgraded to the latest Rust toolchain and compilation times dropped by 40%. The incremental compilation improvements are no joke. Anyone else benchmarking their projects after the update?",
            "subreddit": "programming",
            "imageUrl": "https://images.unsplash.com/photo-1629654297299-c8506221ca97?w=800&q=80",
            "imageCaption": "A developer workspace with multiple monitors showing code editors and terminal windows",
            "createdAt": now - 5400000,
        },
        {
            "postId": "seed-post-02",
            "userId": "seed-user-02",
            "username": "CosmicExplorer",
            "title": "James Webb just captured this image of the Pillars of Creation",
            "postText": "NASA released a new near-infrared image of the Pillars of Creation in the Eagle Nebula. The level of detail is absolutely breathtaking - you can see individual stars forming inside the dust columns. We're literally watching stellar nurseries in action.",
            "subreddit": "space",
            "imageUrl": "https://images.unsplash.com/photo-1462331940025-496dfbfc7564?w=800&q=80",
            "imageCaption": "A deep space nebula with vibrant purple and blue clouds of gas and dust with stars scattered throughout",
            "createdAt": now - 4800000,
        },
        {
            "postId": "seed-post-03",
            "userId": "seed-user-03",
            "username": "PixelWanderer",
            "title": "Cyberpunk 2077 after the 2.0 update is a completely different game",
            "postText": "I gave up on this game at launch but decided to give it another shot after 2.0 and Phantom Liberty. The police system actually works now, vehicle combat is fun, and the skill tree rework makes builds feel meaningful. CDPR really pulled a No Man's Sky here.",
            "subreddit": "gaming",
            "imageUrl": "https://images.unsplash.com/photo-1542751371-adc38448a05e?w=800&q=80",
            "imageCaption": "A gaming setup with RGB lighting showing a neon-lit cyberpunk cityscape on an ultrawide monitor",
            "createdAt": now - 4200000,
        },
        {
            "postId": "seed-post-04",
            "userId": "seed-user-04",
            "username": "DataNerd42",
            "title": "Built a real-time data pipeline with Kafka + Spark and it actually works",
            "postText": "After months of reading about stream processing, I finally built a working pipeline: Kafka ingests events, Spark Structured Streaming processes them in micro-batches, and results land in HDFS. The hardest part was honestly getting the serialization right between Scala and Python services. Happy to answer questions about the architecture!",
            "subreddit": "dataengineering",
            "imageUrl": "https://images.unsplash.com/photo-1551288049-bebda4e38f71?w=800&q=80",
            "imageCaption": "A data visualization dashboard showing real-time streaming metrics with colorful charts and graphs",
            "createdAt": now - 3600000,
        },
        {
            "postId": "seed-post-05",
            "userId": "seed-user-05",
            "username": "CuriousCat99",
            "title": "Why does time feel like it moves faster as you get older?",
            "postText": "When I was a kid, summer felt like it lasted forever. Now entire months disappear before I notice. I read that it might be because each year becomes a smaller fraction of your total life experience, but that explanation doesn't fully satisfy me. Is there a neurological basis for this?",
            "subreddit": "askscience",
            "imageUrl": "",
            "imageCaption": "",
            "createdAt": now - 3000000,
        },
        {
            "postId": "seed-post-06",
            "userId": "seed-user-06",
            "username": "NightOwlDev",
            "title": "My mass-transit city in Cities Skylines 2 - zero car infrastructure",
            "postText": "After 200 hours I finally built a city of 500k with zero personal car infrastructure. Everything runs on metro, trams, buses, and bike lanes. Traffic flow is at 89% and citizen happiness is maxed. The trick is layering your transit networks so every district has at least 2 modes of public transport within walking distance.",
            "subreddit": "gaming",
            "imageUrl": "https://images.unsplash.com/photo-1480714378408-67cf0d13bc1b?w=800&q=80",
            "imageCaption": "An aerial view of a modern city with complex highway interchanges and dense urban development at sunset",
            "createdAt": now - 2400000,
        },
    ]
    for p in seed_posts:
        conn.execute(
            """INSERT OR IGNORE INTO posts
               (postId, userId, username, title, postText, subreddit, imageUrl, imageCaption, createdAt)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                p["postId"],
                p["userId"],
                p["username"],
                p["title"],
                p["postText"],
                p["subreddit"],
                p["imageUrl"],
                p["imageCaption"],
                p["createdAt"],
            ),
        )

    # ---- seed comments ----
    seed_comments = [
        # Post 1 - Rust compiler
        {
            "commentId": "seed-comment-01",
            "postId": "seed-post-01",
            "userId": "seed-user-04",
            "username": "DataNerd42",
            "commentText": "Can confirm. Our CI pipeline went from 12 minutes to about 7 after the upgrade. The parallel frontend work is paying off big time.",
            "parentCommentId": "",
            "createdAt": now - 5300000,
        },
        {
            "commentId": "seed-comment-02",
            "postId": "seed-post-01",
            "userId": "seed-user-06",
            "username": "NightOwlDev",
            "commentText": "I'm still on an older version because some of our dependencies haven't updated yet. Is it safe to jump straight to the latest or should I go incremental?",
            "parentCommentId": "",
            "createdAt": now - 5200000,
        },
        # Post 2 - James Webb
        {
            "commentId": "seed-comment-03",
            "postId": "seed-post-02",
            "userId": "seed-user-05",
            "username": "CuriousCat99",
            "commentText": "The fact that we can see stars being born in real time (astronomically speaking) is mind-blowing. Each one of those pillars is about 5 light-years tall.",
            "parentCommentId": "",
            "createdAt": now - 4700000,
        },
        {
            "commentId": "seed-comment-04",
            "postId": "seed-post-02",
            "userId": "seed-user-01",
            "username": "TechEnthusiast",
            "commentText": "What I find fascinating is that the Pillars of Creation were likely destroyed by a supernova about 6,000 years ago - we just haven't seen the light from that event reach us yet.",
            "parentCommentId": "",
            "createdAt": now - 4600000,
        },
        {
            "commentId": "seed-comment-05",
            "postId": "seed-post-02",
            "userId": "seed-user-03",
            "username": "PixelWanderer",
            "commentText": "Does anyone know if there's a full-resolution version available for download? Would love this as a wallpaper.",
            "parentCommentId": "",
            "createdAt": now - 4500000,
        },
        # Post 3 - Cyberpunk
        {
            "commentId": "seed-comment-06",
            "postId": "seed-post-03",
            "userId": "seed-user-06",
            "username": "NightOwlDev",
            "commentText": "Phantom Liberty is genuinely one of the best DLCs I've ever played. The spy thriller storyline with Idris Elba is incredible. Just wish they hadn't cut the subway system.",
            "parentCommentId": "",
            "createdAt": now - 4100000,
        },
        {
            "commentId": "seed-comment-07",
            "postId": "seed-post-03",
            "userId": "seed-user-02",
            "username": "CosmicExplorer",
            "commentText": "The netrunner build after 2.0 is absolutely broken in the best way. You can hack entire buildings without entering them.",
            "parentCommentId": "",
            "createdAt": now - 4000000,
        },
        # Post 4 - Data pipeline
        {
            "commentId": "seed-comment-08",
            "postId": "seed-post-04",
            "userId": "seed-user-01",
            "username": "TechEnthusiast",
            "commentText": "Nice architecture! What serialization format did you end up using between Kafka and Spark? We tried Avro but the schema registry added a lot of complexity.",
            "parentCommentId": "",
            "createdAt": now - 3500000,
        },
        {
            "commentId": "seed-comment-09",
            "postId": "seed-post-04",
            "userId": "seed-user-05",
            "username": "CuriousCat99",
            "commentText": "How does this handle backpressure? If Spark falls behind, does Kafka just buffer everything or do you have some dropping strategy?",
            "parentCommentId": "",
            "createdAt": now - 3400000,
        },
        # Post 5 - Time perception
        {
            "commentId": "seed-comment-10",
            "postId": "seed-post-05",
            "userId": "seed-user-02",
            "username": "CosmicExplorer",
            "commentText": "There's actually research suggesting that our brains process familiar experiences faster, creating fewer new memories. Since adults encounter fewer truly novel situations, time seems to compress. That's why vacations to new places feel longer.",
            "parentCommentId": "",
            "createdAt": now - 2900000,
        },
        {
            "commentId": "seed-comment-11",
            "postId": "seed-post-05",
            "userId": "seed-user-04",
            "username": "DataNerd42",
            "commentText": "I think about this way too often. Apparently the 'proportional theory' goes back to Paul Janet in 1897. A year at age 5 is 20% of your life, but at 50 it's only 2%.",
            "parentCommentId": "",
            "createdAt": now - 2800000,
        },
        # Post 6 - Cities Skylines
        {
            "commentId": "seed-comment-12",
            "postId": "seed-post-06",
            "userId": "seed-user-03",
            "username": "PixelWanderer",
            "commentText": "89% traffic flow with zero cars is insane. Can you share your transit map? I always struggle with connecting outer districts without creating bottlenecks at central stations.",
            "parentCommentId": "",
            "createdAt": now - 2300000,
        },
        {
            "commentId": "seed-comment-13",
            "postId": "seed-post-06",
            "userId": "seed-user-05",
            "username": "CuriousCat99",
            "commentText": "This is basically what every urban planner dreams of. If only real cities could be redesigned this easily. The Netherlands comes close though.",
            "parentCommentId": "",
            "createdAt": now - 2200000,
        },
    ]
    for c in seed_comments:
        conn.execute(
            """INSERT OR IGNORE INTO comments
               (commentId, postId, userId, username, commentText, parentCommentId, createdAt)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                c["commentId"],
                c["postId"],
                c["userId"],
                c["username"],
                c["commentText"],
                c["parentCommentId"],
                c["createdAt"],
            ),
        )

    conn.commit()
    conn.close()
    logger.info("Database seeded with starter posts and comments")


# ============================================================
# Lifespan
# ============================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan: init DB, start/stop Kafka producer and consumer."""
    global kafka_producer

    # Initialize database and seed starter content
    init_db()
    seed_db()

    # Ensure images directory exists
    os.makedirs(IMAGES_DIR, exist_ok=True)

    # Start Kafka producer
    logger.info("Starting Kafka producer...", bootstrap=KAFKA_BOOTSTRAP)
    kafka_producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )

    retries = 0
    while retries < 10:
        try:
            await kafka_producer.start()
            logger.info("Kafka producer started successfully")
            break
        except Exception as e:
            retries += 1
            logger.warn(f"Kafka connection attempt {retries} failed: {e}")
            await asyncio.sleep(5)

    # Start Kafka replies consumer in background
    asyncio.create_task(consume_replies())

    # Start background retry loop for stuck batch jobs
    asyncio.create_task(retry_stuck_batch_jobs())

    yield

    # Shutdown
    if kafka_producer:
        await kafka_producer.stop()
    logger.info("API Gateway shutting down")


app = FastAPI(
    title="Social Media Platform API",
    description="API Gateway for the Big Data Reply Generation System",
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS + ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve uploaded images as static files
os.makedirs(IMAGES_DIR, exist_ok=True)
app.mount("/api/images", StaticFiles(directory=IMAGES_DIR), name="images")


# ============================================================
# Pydantic Models
# ============================================================
class UserLogin(BaseModel):
    username: str


class UserResponse(BaseModel):
    userId: str
    username: str
    createdAt: int


class PostCreate(BaseModel):
    title: str
    postText: str
    subreddit: str = "general"
    imageUrl: str = ""
    imageCaption: str = ""


class PostResponse(BaseModel):
    postId: str
    userId: str
    username: str
    title: str
    postText: str
    subreddit: str
    imageUrl: str
    imageCaption: str
    createdAt: int
    commentCount: int = 0
    replyCount: int = 0


class CommentCreate(BaseModel):
    postId: str
    commentText: str
    parentCommentId: str = ""


class CommentResponse(BaseModel):
    commentId: str
    postId: str
    userId: str
    username: str
    commentText: str
    parentCommentId: str
    createdAt: int


class ReplyResponse(BaseModel):
    replyId: str
    commentId: str
    postId: str
    generatedReply: str
    timestamp: int


class BatchJobResponse(BaseModel):
    jobId: str
    status: str
    message: str = ""


# ============================================================
# Auth Helper
# ============================================================
def get_current_user(x_user_id: str = Header(None)):
    """Extract user from X-User-Id header."""
    if not x_user_id:
        raise HTTPException(401, "Missing X-User-Id header. Please log in.")
    conn = get_db()
    row = conn.execute("SELECT * FROM users WHERE userId = ?", (x_user_id,)).fetchone()
    conn.close()
    if not row:
        raise HTTPException(401, "Invalid user. Please log in again.")
    return dict(row)


# ============================================================
# Health Check
# ============================================================
@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "service": "api-gateway",
        "kafka_connected": kafka_producer is not None,
        "websocket_clients": len(ws_connections),
    }


# ============================================================
# Auth Endpoints
# ============================================================
@app.post("/api/auth/login", response_model=UserResponse)
async def login(req: UserLogin):
    """Login or register with a username. Returns existing user or creates new one."""
    username = req.username.strip()
    if not username or len(username) < 2:
        raise HTTPException(400, "Username must be at least 2 characters")
    if len(username) > 30:
        raise HTTPException(400, "Username must be at most 30 characters")

    conn = get_db()
    # Check if user exists
    row = conn.execute("SELECT * FROM users WHERE username = ?", (username,)).fetchone()
    if row:
        conn.close()
        logger.info("User logged in", userId=row["userId"], username=username)
        return UserResponse(**dict(row))

    # Create new user
    user_id = str(uuid.uuid4())
    now = int(time.time() * 1000)
    conn.execute(
        "INSERT INTO users (userId, username, createdAt) VALUES (?, ?, ?)",
        (user_id, username, now),
    )
    conn.commit()
    conn.close()
    logger.info("User created and logged in", userId=user_id, username=username)
    return UserResponse(userId=user_id, username=username, createdAt=now)


@app.get("/api/auth/me")
async def get_me(user=Depends(get_current_user)):
    """Get current user info."""
    return user


@app.get("/api/users")
async def list_users():
    conn = get_db()
    rows = conn.execute("SELECT * FROM users ORDER BY createdAt DESC").fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ============================================================
# Image Upload
# ============================================================
ALLOWED_IMAGE_TYPES = {"image/jpeg", "image/png", "image/gif", "image/webp"}
MAX_IMAGE_SIZE = 10 * 1024 * 1024  # 10 MB


@app.post("/api/upload-image")
async def upload_image(file: UploadFile = File(...), user=Depends(get_current_user)):
    """Upload an image file. Returns the URL to access it."""
    if file.content_type not in ALLOWED_IMAGE_TYPES:
        raise HTTPException(
            400,
            f"Invalid image type: {file.content_type}. Allowed: JPEG, PNG, GIF, WebP",
        )

    content = await file.read()
    if len(content) > MAX_IMAGE_SIZE:
        raise HTTPException(
            400,
            f"Image too large. Maximum size is {MAX_IMAGE_SIZE // (1024 * 1024)} MB",
        )

    # Generate unique filename
    ext = file.filename.rsplit(".", 1)[-1] if "." in file.filename else "jpg"
    filename = f"{uuid.uuid4()}.{ext}"
    filepath = os.path.join(IMAGES_DIR, filename)

    async with aiofiles.open(filepath, "wb") as f:
        await f.write(content)

    image_url = f"/api/images/{filename}"
    logger.info("Image uploaded", filename=filename, size=len(content), url=image_url)

    return {"imageUrl": image_url, "filename": filename, "size": len(content)}


# ============================================================
# Post Endpoints
# ============================================================
@app.post("/api/posts", response_model=PostResponse)
async def create_post(post: PostCreate, user=Depends(get_current_user)):
    post_id = str(uuid.uuid4())
    now = int(time.time() * 1000)

    image_caption = post.imageCaption

    # Auto-generate image caption if image is provided but no caption given
    if post.imageUrl and not image_caption:
        try:
            request_body = {
                "image_url": post.imageUrl,
                "context": f"{post.title} - {post.postText}",
            }

            # For local images, read the file and send as base64
            if post.imageUrl.startswith("/api/images/"):
                import base64
                import mimetypes

                filename = post.imageUrl.split("/")[-1]
                local_path = os.path.join(IMAGES_DIR, filename)
                if os.path.exists(local_path):
                    mime_type = mimetypes.guess_type(local_path)[0] or "image/jpeg"
                    with open(local_path, "rb") as img_file:
                        img_data = base64.b64encode(img_file.read()).decode("utf-8")
                    request_body["image_base64"] = f"data:{mime_type};base64,{img_data}"

            async with httpx.AsyncClient(timeout=60) as client:
                resp = await client.post(
                    f"{INFERENCE_URL}/describe-image",
                    json=request_body,
                )
                if resp.status_code == 200:
                    caption_data = resp.json()
                    image_caption = caption_data.get("caption", "")
                    logger.info(
                        "Auto-generated image caption",
                        postId=post_id,
                        caption=image_caption[:80],
                    )
        except Exception as e:
            logger.error("Failed to auto-generate image caption", error=str(e))

    post_data = {
        "postId": post_id,
        "userId": user["userId"],
        "username": user["username"],
        "title": post.title,
        "postText": post.postText,
        "subreddit": post.subreddit,
        "imageUrl": post.imageUrl,
        "imageCaption": image_caption,
        "createdAt": now,
    }

    conn = get_db()
    conn.execute(
        """INSERT INTO posts (postId, userId, username, title, postText, subreddit, imageUrl, imageCaption, createdAt)
                    VALUES (:postId, :userId, :username, :title, :postText, :subreddit, :imageUrl, :imageCaption, :createdAt)""",
        post_data,
    )
    conn.commit()
    conn.close()

    # Publish to Kafka posts_topic
    if kafka_producer:
        await kafka_producer.send("posts_topic", key=post_id, value=post_data)
        logger.info("Post published to Kafka", postId=post_id)

    return PostResponse(**post_data, commentCount=0, replyCount=0)


@app.get("/api/posts")
async def list_posts():
    conn = get_db()
    rows = conn.execute("""
        SELECT p.*,
               (SELECT COUNT(*) FROM comments c WHERE c.postId = p.postId) as commentCount,
               (SELECT COUNT(*) FROM replies r WHERE r.postId = p.postId) as replyCount
        FROM posts p
        ORDER BY p.createdAt DESC
        LIMIT 100
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@app.get("/api/posts/{post_id}")
async def get_post(post_id: str):
    conn = get_db()
    row = conn.execute(
        """
        SELECT p.*,
               (SELECT COUNT(*) FROM comments c WHERE c.postId = p.postId) as commentCount,
               (SELECT COUNT(*) FROM replies r WHERE r.postId = p.postId) as replyCount
        FROM posts p WHERE p.postId = ?
    """,
        (post_id,),
    ).fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, "Post not found")
    return dict(row)


# ============================================================
# Comment Endpoints (-> Kafka)
# ============================================================
@app.post("/api/comments", response_model=CommentResponse)
async def create_comment(comment: CommentCreate, user=Depends(get_current_user)):
    comment_id = str(uuid.uuid4())
    now = int(time.time() * 1000)

    # Get post info for the Kafka message (Spark needs post context)
    conn = get_db()
    post_row = conn.execute(
        "SELECT * FROM posts WHERE postId = ?", (comment.postId,)
    ).fetchone()
    if not post_row:
        conn.close()
        raise HTTPException(404, "Post not found")
    post = dict(post_row)

    comment_data = {
        "commentId": comment_id,
        "postId": comment.postId,
        "userId": user["userId"],
        "username": user["username"],
        "commentText": comment.commentText,
        "parentCommentId": comment.parentCommentId,
        "createdAt": now,
    }

    conn.execute(
        """INSERT INTO comments (commentId, postId, userId, username, commentText, parentCommentId, createdAt)
                    VALUES (:commentId, :postId, :userId, :username, :commentText, :parentCommentId, :createdAt)""",
        comment_data,
    )
    conn.commit()
    conn.close()

    # Build Kafka message with post context for Spark preprocessing
    kafka_message = {
        "commentId": comment_id,
        "postId": comment.postId,
        "userId": user["userId"],
        "username": user["username"],
        "postText": post.get("postText", ""),
        "commentText": comment.commentText,
        "imageUrl": post.get("imageUrl", ""),
        "imageCaption": post.get("imageCaption", ""),
        "subreddit": post.get("subreddit", "general"),
        "timestamp": now,
        "parentCommentId": comment.parentCommentId,
        "upvotes": 0,
        "metadata": {},
    }

    if kafka_producer:
        await kafka_producer.send("comments_topic", key=comment_id, value=kafka_message)
        logger.info(
            "Comment published to Kafka", commentId=comment_id, postId=comment.postId
        )

    return CommentResponse(**comment_data)


@app.get("/api/comments/{post_id}")
async def get_comments(post_id: str):
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM comments WHERE postId = ? ORDER BY createdAt ASC", (post_id,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ============================================================
# Replies Endpoints
# ============================================================
@app.get("/api/replies")
async def list_replies():
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM replies ORDER BY timestamp DESC LIMIT 100"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@app.get("/api/replies/{post_id}")
async def get_post_replies(post_id: str):
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM replies WHERE postId = ? ORDER BY timestamp ASC", (post_id,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ============================================================
# Bulk Upload (-> HDFS -> Spark Batch)
# ============================================================
@app.post("/api/bulk/upload")
async def bulk_upload(file: UploadFile = File(...), user=Depends(get_current_user)):
    """Upload a bulk JSON file for batch processing."""
    if not file.filename.endswith(".json"):
        raise HTTPException(400, "Only JSON files are supported")

    job_id = str(uuid.uuid4())
    upload_dir = "/tmp/uploads"
    os.makedirs(upload_dir, exist_ok=True)

    local_path = f"{upload_dir}/{job_id}.json"
    hdfs_input_path = f"/data/raw/bulk/{job_id}"
    hdfs_output_path = f"/data/replies/bulk/{job_id}"

    # Save file locally
    content = await file.read()
    async with aiofiles.open(local_path, "wb") as f:
        await f.write(content)

    logger.info(
        "Bulk file uploaded", jobId=job_id, filename=file.filename, size=len(content)
    )

    # Store job in DB
    conn = get_db()
    conn.execute(
        """INSERT INTO batch_jobs (jobId, status, inputPath, outputPath, uploadedAt, filename)
                    VALUES (?, ?, ?, ?, ?, ?)""",
        (
            job_id,
            "uploading",
            hdfs_input_path,
            hdfs_output_path,
            int(time.time() * 1000),
            file.filename,
        ),
    )
    conn.commit()
    conn.close()

    # Upload to HDFS via WebHDFS
    try:
        async with httpx.AsyncClient(timeout=60) as client:
            create_url = (
                f"{HDFS_URL}/webhdfs/v1{hdfs_input_path}?op=MKDIRS&user.name=root"
            )
            await client.put(create_url)

            # Step 1: Request the upload location (noredirect to get DataNode URL)
            upload_url = f"{HDFS_URL}/webhdfs/v1{hdfs_input_path}/data.json?op=CREATE&user.name=root&overwrite=true&noredirect=true"
            resp = await client.put(upload_url)

            # Step 2: Extract the DataNode redirect URL and send the actual data
            redirect_url = None
            if resp.status_code == 307:
                redirect_url = resp.headers.get("Location", "")
            elif resp.status_code in (200, 201):
                # noredirect=true returns 200 with Location in JSON body
                try:
                    location_data = resp.json()
                    redirect_url = location_data.get("Location", "")
                except Exception:
                    redirect_url = resp.headers.get("Location", "")

            if redirect_url:
                await client.put(
                    redirect_url,
                    content=content,
                    headers={"Content-Type": "application/octet-stream"},
                )
            else:
                # Fallback: direct two-step upload without noredirect
                step1_url = f"{HDFS_URL}/webhdfs/v1{hdfs_input_path}/data.json?op=CREATE&user.name=root&overwrite=true"
                step1_resp = await client.put(step1_url, follow_redirects=False)
                if step1_resp.status_code == 307:
                    datanode_url = step1_resp.headers.get("Location", "")
                    if datanode_url:
                        await client.put(
                            datanode_url,
                            content=content,
                            headers={"Content-Type": "application/octet-stream"},
                        )

            logger.info("File uploaded to HDFS", path=hdfs_input_path)

    except Exception as e:
        logger.error("HDFS upload failed, using local file", error=str(e))

    # Trigger Spark batch job (with retries)
    status = "uploaded"
    spark_job_id = ""
    for attempt in range(3):
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.post(
                    f"{SPARK_BATCH_URL}/process",
                    json={
                        "input_path": hdfs_input_path,
                        "output_path": hdfs_output_path,
                    },
                )
                if resp.status_code == 200:
                    spark_job = resp.json()
                    spark_job_id = spark_job.get("job_id", "")
                    status = "processing"
                    logger.info(
                        "Spark batch job triggered", jobId=job_id, attempt=attempt + 1
                    )
                    break
                else:
                    status = "submitted"
        except Exception as e:
            logger.warning(
                "Failed to trigger Spark batch", error=str(e), attempt=attempt + 1
            )
            status = "uploaded"
            if attempt < 2:
                await asyncio.sleep(5 * (attempt + 1))

    # Update job status
    conn = get_db()
    conn.execute(
        "UPDATE batch_jobs SET status = ?, sparkJobId = ? WHERE jobId = ?",
        (status, spark_job_id, job_id),
    )
    conn.commit()
    conn.close()

    return BatchJobResponse(
        jobId=job_id,
        status=status,
        message=f"File '{file.filename}' uploaded and processing started",
    )


@app.get("/api/bulk/status/{job_id}")
async def bulk_status(job_id: str):
    conn = get_db()
    row = conn.execute("SELECT * FROM batch_jobs WHERE jobId = ?", (job_id,)).fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, "Job not found")

    job = dict(row)

    # Auto-retry stuck "uploaded" jobs (Spark wasn't reachable during upload)
    if job.get("status") in ("uploaded", "submitted") and not job.get("sparkJobId"):
        hdfs_input_path = job.get("inputPath", "")
        hdfs_output_path = job.get("outputPath", "")
        if hdfs_input_path and hdfs_output_path:
            try:
                async with httpx.AsyncClient(timeout=10) as client:
                    resp = await client.post(
                        f"{SPARK_BATCH_URL}/process",
                        json={
                            "input_path": hdfs_input_path,
                            "output_path": hdfs_output_path,
                        },
                    )
                    if resp.status_code == 200:
                        spark_job = resp.json()
                        spark_job_id = spark_job.get("job_id", "")
                        conn2 = get_db()
                        conn2.execute(
                            "UPDATE batch_jobs SET status = 'processing', sparkJobId = ? WHERE jobId = ?",
                            (spark_job_id, job_id),
                        )
                        conn2.commit()
                        conn2.close()
                        job["status"] = "processing"
                        job["sparkJobId"] = spark_job_id
                        logger.info(
                            "Auto-retried stuck batch job",
                            jobId=job_id,
                            sparkJobId=spark_job_id,
                        )
            except Exception as e:
                logger.debug(
                    "Auto-retry failed for stuck batch job", jobId=job_id, error=str(e)
                )

    # Check Spark job status and sync record counts
    spark_job_id = job.get("sparkJobId", "")
    if spark_job_id:
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(f"{SPARK_BATCH_URL}/status/{spark_job_id}")
                if resp.status_code == 200:
                    spark_status = resp.json()
                    new_status = spark_status.get("status", job["status"])
                    total_records = spark_status.get("totalRecords", 0)
                    processed_records = spark_status.get("processedRecords", 0)
                    needs_update = (
                        new_status != job["status"]
                        or total_records != job.get("totalRecords", 0)
                        or processed_records != job.get("processedRecords", 0)
                    )
                    if needs_update:
                        conn2 = get_db()
                        conn2.execute(
                            "UPDATE batch_jobs SET status = ?, totalRecords = ?, processedRecords = ? WHERE jobId = ?",
                            (new_status, total_records, processed_records, job_id),
                        )
                        conn2.commit()
                        conn2.close()
                        job["status"] = new_status
                        job["totalRecords"] = total_records
                        job["processedRecords"] = processed_records
        except Exception:
            pass

    return job


@app.get("/api/bulk/download/{job_id}")
async def bulk_download(job_id: str):
    conn = get_db()
    row = conn.execute("SELECT * FROM batch_jobs WHERE jobId = ?", (job_id,)).fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, "Job not found")

    job = dict(row)
    output_path = job.get("outputPath", "")

    # Try to read from HDFS
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            list_url = (
                f"{HDFS_URL}/webhdfs/v1{output_path}?op=LISTSTATUS&user.name=root"
            )
            resp = await client.get(list_url)

            if resp.status_code == 200:
                files_data = resp.json()
                file_statuses = files_data.get("FileStatuses", {}).get("FileStatus", [])

                all_data = []
                for file_info in file_statuses:
                    if file_info["pathSuffix"].endswith(".json"):
                        read_url = f"{HDFS_URL}/webhdfs/v1{output_path}/{file_info['pathSuffix']}?op=OPEN&user.name=root&noredirect=true"
                        file_resp = await client.get(read_url)
                        # Handle WebHDFS redirect to DataNode
                        redirect_url = None
                        if file_resp.status_code == 307:
                            redirect_url = file_resp.headers.get("Location", "")
                        elif file_resp.status_code in (200, 201):
                            try:
                                location_data = file_resp.json()
                                redirect_url = location_data.get("Location", "")
                            except Exception:
                                redirect_url = file_resp.headers.get("Location", "")
                        if redirect_url:
                            file_resp = await client.get(redirect_url)
                        for line in file_resp.text.strip().split("\n"):
                            if line.strip():
                                try:
                                    all_data.append(json.loads(line))
                                except json.JSONDecodeError:
                                    pass

                if all_data:
                    download_path = f"/tmp/downloads/{job_id}_replies.json"
                    os.makedirs("/tmp/downloads", exist_ok=True)
                    async with aiofiles.open(download_path, "w") as f:
                        await f.write(json.dumps(all_data, indent=2))

                    return FileResponse(
                        path=download_path,
                        filename=f"replies_{job_id}.json",
                        media_type="application/json",
                    )

    except Exception as e:
        logger.error("Failed to read from HDFS", error=str(e))

    raise HTTPException(404, "Output files not yet available")


@app.get("/api/bulk/jobs")
async def list_batch_jobs():
    conn = get_db()
    rows = conn.execute("SELECT * FROM batch_jobs ORDER BY uploadedAt DESC").fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ============================================================
# WebSocket for Live Replies
# ============================================================
@app.websocket("/ws/replies")
async def websocket_replies(websocket: WebSocket):
    await websocket.accept()
    ws_connections.add(websocket)
    logger.info("WebSocket client connected", total=len(ws_connections))

    try:
        while True:
            data = await websocket.receive_text()
            logger.debug("WebSocket message received", data=data)
    except WebSocketDisconnect:
        ws_connections.discard(websocket)
        logger.info("WebSocket client disconnected", total=len(ws_connections))


async def broadcast_reply(reply_data: dict):
    if not ws_connections:
        return

    message = json.dumps(reply_data)
    disconnected = set()

    for ws in ws_connections:
        try:
            await ws.send_text(message)
        except Exception:
            disconnected.add(ws)

    ws_connections.difference_update(disconnected)


# ============================================================
# Background: Retry stuck batch jobs
# ============================================================
async def retry_stuck_batch_jobs():
    """Periodically check for batch jobs stuck in 'uploaded' status and retry triggering Spark."""
    await asyncio.sleep(30)  # Initial delay to let services start
    while True:
        try:
            conn = get_db()
            rows = conn.execute(
                "SELECT * FROM batch_jobs WHERE status IN ('uploaded', 'submitted') AND (sparkJobId IS NULL OR sparkJobId = '')"
            ).fetchall()
            conn.close()

            for row in rows:
                job = dict(row)
                job_id = job["jobId"]
                hdfs_input_path = job.get("inputPath", "")
                hdfs_output_path = job.get("outputPath", "")
                if not hdfs_input_path or not hdfs_output_path:
                    continue
                try:
                    async with httpx.AsyncClient(timeout=10) as client:
                        resp = await client.post(
                            f"{SPARK_BATCH_URL}/process",
                            json={
                                "input_path": hdfs_input_path,
                                "output_path": hdfs_output_path,
                            },
                        )
                        if resp.status_code == 200:
                            spark_job = resp.json()
                            spark_job_id = spark_job.get("job_id", "")
                            conn2 = get_db()
                            conn2.execute(
                                "UPDATE batch_jobs SET status = 'processing', sparkJobId = ? WHERE jobId = ?",
                                (spark_job_id, job_id),
                            )
                            conn2.commit()
                            conn2.close()
                            logger.info(
                                "Background retry: triggered stuck batch job",
                                jobId=job_id,
                                sparkJobId=spark_job_id,
                            )
                except Exception as e:
                    logger.debug(
                        "Background retry failed for batch job",
                        jobId=job_id,
                        error=str(e),
                    )
        except Exception as e:
            logger.debug("Background retry loop error", error=str(e))

        await asyncio.sleep(30)  # Check every 30 seconds


# ============================================================
# Kafka Replies Consumer (Background Task)
# ============================================================
async def consume_replies():
    logger.info("Starting Kafka replies consumer...")

    await asyncio.sleep(10)

    consumer = AIOKafkaConsumer(
        "replies_topic",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="api-gateway-replies-consumer",
        auto_offset_reset="latest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    retries = 0
    while retries < 15:
        try:
            await consumer.start()
            logger.info("Kafka replies consumer started")
            break
        except Exception as e:
            retries += 1
            logger.warn(f"Kafka consumer attempt {retries} failed: {e}")
            await asyncio.sleep(5)
    else:
        logger.error("Failed to start Kafka replies consumer after 15 attempts")
        return

    try:
        async for message in consumer:
            try:
                reply_data = message.value
                reply_id = reply_data.get("replyId", str(uuid.uuid4()))

                # Store reply in database
                conn = get_db()
                conn.execute(
                    """INSERT OR IGNORE INTO replies (replyId, commentId, postId, generatedReply, timestamp)
                               VALUES (?, ?, ?, ?, ?)""",
                    (
                        reply_id,
                        reply_data.get("commentId", ""),
                        reply_data.get("postId", ""),
                        reply_data.get("generatedReply", ""),
                        reply_data.get("timestamp", int(time.time() * 1000)),
                    ),
                )
                conn.commit()
                conn.close()

                logger.info("Reply received from Kafka and stored", replyId=reply_id)

                # Broadcast to WebSocket clients
                await broadcast_reply(reply_data)

            except Exception as e:
                logger.error("Error processing Kafka reply", error=str(e))
    finally:
        await consumer.stop()


# ============================================================
# Stats Endpoint
# ============================================================
@app.get("/api/stats")
async def get_stats():
    conn = get_db()
    stats = {
        "users": conn.execute("SELECT COUNT(*) FROM users").fetchone()[0],
        "posts": conn.execute("SELECT COUNT(*) FROM posts").fetchone()[0],
        "comments": conn.execute("SELECT COUNT(*) FROM comments").fetchone()[0],
        "replies": conn.execute("SELECT COUNT(*) FROM replies").fetchone()[0],
        "batchJobs": conn.execute("SELECT COUNT(*) FROM batch_jobs").fetchone()[0],
        "wsClients": len(ws_connections),
        "kafkaConnected": kafka_producer is not None,
    }
    conn.close()
    return stats


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8001)
