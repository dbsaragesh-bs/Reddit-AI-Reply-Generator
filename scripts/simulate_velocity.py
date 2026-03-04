"""
Velocity Simulation Script

Simulates high-velocity comment streaming at configurable rates.
Demonstrates the Velocity dimension of Big Data.

Usage:
    python simulate_velocity.py --rate 100 --duration 60
    python simulate_velocity.py --rate 1000 --duration 30
"""

import argparse
import asyncio
import json
import random
import time
import uuid
import sys

import httpx

API_URL = "http://localhost:8001"

# Sample data for realistic simulation
SUBREDDITS = [
    "technology", "programming", "datascience", "machinelearning",
    "artificial", "python", "scala", "bigdata", "cloudcomputing",
    "webdev", "devops", "cybersecurity", "gamedev", "linux",
    "science", "news", "worldnews", "askreddit", "todayilearned"
]

USERNAMES = [
    "data_wizard", "spark_master", "kafka_consumer", "hdfs_hero",
    "ml_engineer", "code_ninja", "byte_cruncher", "algo_expert",
    "cloud_native", "devops_pro", "big_data_fan", "ai_enthusiast",
    "neural_net", "deep_learning", "stream_processor", "batch_boss",
    "scala_dev", "python_guru", "java_jedi", "rust_warrior"
]

POST_TITLES = [
    "The future of distributed computing",
    "Why Kafka is essential for real-time data pipelines",
    "Spark vs Flink: A comprehensive comparison",
    "Building scalable microservices with containers",
    "The rise of LLMs in production systems",
    "HDFS architecture deep dive",
    "Stream processing patterns for big data",
    "Scala for data engineering: Best practices",
    "Machine learning at scale with Spark MLlib",
    "Real-time analytics with Apache Kafka Streams",
    "Data lake vs data warehouse: Which to choose?",
    "Kubernetes for big data workloads",
    "Event-driven architecture patterns",
    "Building a real-time recommendation engine",
    "The 5 V's of Big Data explained",
]

COMMENT_TEMPLATES = [
    "This is a really interesting perspective on {topic}. I think the key challenge is scalability.",
    "Great post! I've been working with {tech} for years and this resonates with my experience.",
    "I disagree with some points. In my experience, {tech} handles this differently.",
    "Has anyone tried using {tech} for this use case? Would love to hear experiences.",
    "The comparison between Spark and Flink is spot on. We migrated to {tech} last year.",
    "This is exactly what we needed for our data pipeline. Thanks for sharing!",
    "I wonder how this scales to millions of events per second. Any benchmarks?",
    "Our team uses {tech} in production and it's been fantastic for real-time processing.",
    "The architecture diagram is very clear. How do you handle fault tolerance?",
    "This reminds me of the CAP theorem trade-offs. You can't have everything.",
    "We process about 10TB daily with a similar setup. Key is proper partitioning.",
    "The Scala preprocessing approach is smart - avoids serialization overhead.",
    "How do you handle backpressure in the Kafka to Spark pipeline?",
    "LLM inference at scale is tricky. Batching is definitely the way to go.",
    "We use a similar architecture but with Flink instead of Spark. Works great!",
]

TECHNOLOGIES = [
    "Spark", "Kafka", "HDFS", "Flink", "Kubernetes", "Docker",
    "TensorFlow", "PyTorch", "Scala", "Python", "Redis", "PostgreSQL"
]


async def create_users(client: httpx.AsyncClient, count: int = 20) -> list:
    """Create simulated users."""
    users = []
    for username in USERNAMES[:count]:
        try:
            resp = await client.post(f"{API_URL}/api/users", json={
                "username": username,
                "email": f"{username}@simulation.test"
            })
            if resp.status_code == 200:
                users.append(resp.json())
        except Exception as e:
            print(f"Failed to create user {username}: {e}")
    return users


async def create_posts(client: httpx.AsyncClient, users: list, count: int = 15) -> list:
    """Create simulated posts."""
    posts = []
    for i, title in enumerate(POST_TITLES[:count]):
        user = random.choice(users)
        try:
            resp = await client.post(f"{API_URL}/api/posts", json={
                "userId": user["userId"],
                "title": title,
                "postText": f"{title}. This is a detailed discussion about modern data engineering practices and distributed systems.",
                "subreddit": random.choice(SUBREDDITS),
                "imageUrl": f"https://example.com/image_{i}.jpg" if random.random() > 0.7 else "",
                "imageCaption": f"Architecture diagram for {random.choice(TECHNOLOGIES)}" if random.random() > 0.7 else ""
            })
            if resp.status_code == 200:
                posts.append(resp.json())
        except Exception as e:
            print(f"Failed to create post: {e}")
    return posts


async def stream_comments(
    client: httpx.AsyncClient,
    users: list,
    posts: list,
    rate: int,
    duration: int
):
    """Stream comments at the specified rate (comments/sec)."""
    if not users or not posts:
        print("Error: No users or posts available!")
        return

    total_sent = 0
    start_time = time.time()
    interval = 1.0 / rate  # Time between comments

    print(f"\n{'='*60}")
    print(f"  VELOCITY SIMULATION")
    print(f"  Rate: {rate} comments/sec")
    print(f"  Duration: {duration} seconds")
    print(f"  Expected total: {rate * duration} comments")
    print(f"{'='*60}\n")

    batch_size = min(rate, 50)  # Send in batches for efficiency
    
    while time.time() - start_time < duration:
        batch_start = time.time()
        tasks = []

        for _ in range(batch_size):
            user = random.choice(users)
            post = random.choice(posts)
            tech = random.choice(TECHNOLOGIES)
            template = random.choice(COMMENT_TEMPLATES)
            comment_text = template.format(topic=post["title"][:30], tech=tech)

            tasks.append(
                client.post(f"{API_URL}/api/comments", json={
                    "userId": user["userId"],
                    "postId": post["postId"],
                    "commentText": comment_text
                })
            )

        # Send batch
        results = await asyncio.gather(*tasks, return_exceptions=True)
        success = sum(1 for r in results if not isinstance(r, Exception) and hasattr(r, 'status_code') and r.status_code == 200)
        total_sent += success

        elapsed = time.time() - start_time
        current_rate = total_sent / max(elapsed, 0.001)

        # Progress update
        sys.stdout.write(f"\r  ⚡ Sent: {total_sent:,} | Rate: {current_rate:.0f}/sec | Time: {elapsed:.0f}s/{duration}s")
        sys.stdout.flush()

        # Throttle to target rate
        batch_elapsed = time.time() - batch_start
        target_elapsed = batch_size / rate
        if batch_elapsed < target_elapsed:
            await asyncio.sleep(target_elapsed - batch_elapsed)

    elapsed = time.time() - start_time
    final_rate = total_sent / max(elapsed, 0.001)

    print(f"\n\n{'='*60}")
    print(f"  SIMULATION COMPLETE")
    print(f"  Total comments sent: {total_sent:,}")
    print(f"  Actual duration: {elapsed:.1f}s")
    print(f"  Average rate: {final_rate:.0f} comments/sec")
    print(f"{'='*60}\n")


async def main():
    parser = argparse.ArgumentParser(description="Simulate high-velocity comment streaming")
    parser.add_argument("--rate", type=int, default=100, help="Comments per second (default: 100)")
    parser.add_argument("--duration", type=int, default=30, help="Duration in seconds (default: 30)")
    parser.add_argument("--api-url", type=str, default=API_URL, help="API Gateway URL")
    args = parser.parse_args()

    global API_URL
    API_URL = args.api_url

    print(f"\n🚀 Reddit Big Data Velocity Simulator")
    print(f"   API: {API_URL}")
    print(f"   Target: {args.rate} comments/sec for {args.duration}s\n")

    async with httpx.AsyncClient(timeout=30) as client:
        # Check API health
        try:
            resp = await client.get(f"{API_URL}/health")
            print(f"✅ API Gateway healthy: {resp.json()}\n")
        except Exception as e:
            print(f"❌ API Gateway not reachable: {e}")
            print("   Make sure services are running: docker-compose up -d")
            return

        # Setup
        print("📝 Creating users...")
        users = await create_users(client)
        print(f"   Created {len(users)} users")

        print("📄 Creating posts...")
        posts = await create_posts(client)
        print(f"   Created {len(posts)} posts")

        # Stream comments
        await stream_comments(client, users, posts, args.rate, args.duration)

        # Final stats
        try:
            stats = await client.get(f"{API_URL}/api/stats")
            print(f"\n📊 Final System Stats:")
            print(f"   {json.dumps(stats.json(), indent=2)}")
        except Exception:
            pass


if __name__ == "__main__":
    asyncio.run(main())
