"""
Bulk Data Generator (Enhanced for Demo)

Generates large JSON files with nested posts and comments
for batch processing demonstration. Includes real image URLs
from Picsum (Lorem Picsum) for visual posts.

Usage:
    python generate_bulk_data.py --count 5000 --output demo_bulk_5k.json
    python generate_bulk_data.py --count 10000 --output demo_bulk_10k.json
"""

import argparse
import json
import random
import time
import uuid
import sys

SUBREDDITS = [
    "technology",
    "programming",
    "datascience",
    "machinelearning",
    "artificial",
    "python",
    "scala",
    "bigdata",
    "cloudcomputing",
    "webdev",
    "devops",
    "cybersecurity",
    "gamedev",
    "linux",
    "science",
    "news",
    "worldnews",
    "askreddit",
    "todayilearned",
    "gaming",
    "music",
    "movies",
    "books",
    "fitness",
    "photography",
    "art",
    "food",
    "travel",
    "nature",
    "space",
    "robotics",
    "startups",
    "entrepreneur",
    "math",
]

USERNAMES = [
    "tech_wizard",
    "code_master",
    "data_guru",
    "ml_expert",
    "cloud_hero",
    "dev_ninja",
    "byte_cruncher",
    "algo_king",
    "spark_fan",
    "kafka_pro",
    "hdfs_user",
    "ai_builder",
    "neural_dev",
    "deep_think",
    "stream_guru",
    "batch_pro",
    "scala_dev",
    "python_guru",
    "java_jedi",
    "rust_warrior",
    "go_phile",
    "node_runner",
    "react_dev",
    "vue_master",
    "angular_pro",
    "docker_fan",
    "k8s_admin",
    "terraform_guru",
    "ansible_user",
    "jenkins_ci",
    "pixel_pusher",
    "debug_king",
    "linux_lord",
    "git_guru",
    "api_architect",
    "db_whisperer",
    "cache_queen",
    "thread_safe",
    "null_pointer",
    "stack_overflow",
    "type_checker",
    "regex_god",
    "vim_user",
    "emacs_rebel",
    "crypto_miner",
    "web3_skeptic",
    "cloud_native",
    "edge_runner",
    "quantum_dev",
    "tensor_flow",
    "gradient_desc",
    "backprop_bob",
]

POST_TOPICS = [
    "distributed systems",
    "machine learning",
    "data engineering",
    "cloud computing",
    "containerization",
    "microservices",
    "real-time processing",
    "batch processing",
    "data lakes",
    "stream processing",
    "API design",
    "database optimization",
    "scalability patterns",
    "fault tolerance",
    "load balancing",
    "caching strategies",
    "message queues",
    "event sourcing",
    "CQRS patterns",
    "serverless architecture",
    "edge computing",
    "quantum computing",
    "blockchain",
    "cybersecurity",
    "DevOps practices",
    "CI/CD pipelines",
    "observability",
    "performance tuning",
    "cost optimization",
    "data governance",
    "LLM inference",
    "RAG pipelines",
    "vector databases",
    "GPU clustering",
    "model fine-tuning",
    "prompt engineering",
    "data mesh",
    "lakehouse architecture",
    "Apache Iceberg",
    "delta lake",
    "Apache Flink",
    "Apache Kafka Streams",
]

# Diverse post titles (not just "Discussion about X")
POST_TITLE_TEMPLATES = [
    "Discussion about {topic} in {subreddit}",
    "Hot take: {topic} is overhyped and here's why",
    "We just migrated to {topic} and the results are insane",
    "Beginner's guide to {topic} - what I wish I knew",
    "Why {topic} will define the next decade of tech",
    "{topic} vs traditional approaches - a real benchmark",
    "Our team's experience with {topic} at scale (lessons learned)",
    "Unpopular opinion: {topic} is not for everyone",
    "ELI5: How does {topic} actually work under the hood?",
    "Just published a paper on {topic} - AMA",
    "PSA: Common pitfalls when implementing {topic}",
    "{topic} in production: what nobody tells you",
    "Built a side project using {topic} over the weekend",
    "Interview prep: {topic} questions that actually matter",
    "The state of {topic} in 2026 - annual review",
]

# Diverse post body templates
POST_BODY_TEMPLATES = [
    "Let's discuss {topic}. This is an important topic in modern software engineering that affects how we build and scale systems. What are your thoughts on the current trends and best practices?",
    "After 3 years of working with {topic}, I wanted to share some insights. The biggest challenge isn't the technology itself, it's the organizational change required. Our team of 15 engineers went through a complete mindset shift.",
    "I've been benchmarking {topic} against our existing solution and the results surprised me. Throughput improved by 340% but latency actually got worse for small payloads. Has anyone else seen this pattern?",
    "Quick question for anyone experienced with {topic}: we're processing about 2M events/day and hitting memory pressure. Our current setup uses 64GB RAM across 4 nodes. Is this expected or are we doing something wrong?",
    "Just wanted to share that {topic} saved our startup. We were burning $40k/month on infra and after the migration it dropped to $8k. The ROI was almost immediate.",
    "I'm writing a blog series on {topic} and wanted to get community input. What are the most misunderstood aspects? What do tutorials always get wrong?",
    "We open-sourced our {topic} implementation today. It handles 500k requests/sec on a single node. Link in comments. Would love feedback from the community.",
    "Controversial take: most companies don't need {topic}. A simple PostgreSQL setup handles 90% of use cases. Fight me in the comments.",
    "Hiring for a {topic} role and struggling to find candidates. The talent pool is incredibly shallow. What's the best way to upskill existing engineers?",
    "Our monitoring showed that {topic} reduced our P99 latency from 2.3s to 180ms. Here's the architecture we used and the tradeoffs we made.",
]

COMMENT_BODIES = [
    "This is a really interesting perspective. I've been working on similar problems and found that {topic} is key.",
    "Great insights! However, I think we should also consider the trade-offs with {topic}.",
    "I disagree with some points here. In my experience, {topic} is more nuanced than described.",
    "Has anyone tried implementing this at scale? Curious about performance with {topic}.",
    "This aligns perfectly with what we've seen in production. {topic} has been a game-changer.",
    "The comparison is spot on. We migrated our entire stack last year and {topic} made it smooth.",
    "I wonder how this handles edge cases, especially around {topic}. Any real-world examples?",
    "Our team has been researching {topic} extensively. Happy to share our findings.",
    "The architecture diagram could benefit from showing {topic} integration points.",
    "This reminds me of a talk I saw about {topic}. The principles are very similar.",
    "We process about 10TB daily and {topic} has been essential for our pipeline.",
    "The Scala approach for preprocessing is smart. It leverages {topic} effectively.",
    "How do you handle backpressure when dealing with {topic} at high throughput?",
    "LLM inference combined with {topic} is the future of intelligent systems.",
    "We use a similar architecture but with different tooling for {topic}. Works great!",
    "Solid post. One thing I'd add is that {topic} works best when combined with proper monitoring.",
    "We tried this exact approach and ran into issues with {topic} at around 100k concurrent users.",
    "This is the kind of content I come to this sub for. Saved for later.",
    "I implemented something similar but used {topic} instead. Curious about the performance difference.",
    "The cost analysis is spot on. {topic} can get expensive real quick if you're not careful.",
    "Have you considered using {topic} for the caching layer? We saw 10x improvement.",
    "tl;dr - use {topic} if you need real-time, use batch processing otherwise. Simple as that.",
    "Our CTO just shared this in slack. Exactly what we needed for our {topic} migration.",
    "Been lurking here for months but had to comment. This nails the {topic} problem perfectly.",
    "Hot take: {topic} is just a fad. Give it 2 years and everyone will move on.",
    "Running this in production with 50 nodes. {topic} handles our 1PB dataset without issues.",
    "The latency numbers you're showing for {topic} seem too good to be true. What's your test setup?",
    "Just deployed this pattern with {topic} yesterday. So far so good but early days.",
    "Anyone know if {topic} supports horizontal scaling? The docs are unclear on this.",
    "We switched from X to {topic} and our on-call incidents dropped by 70%. Not even exaggerating.",
]

# Dirty/spam comments for preprocessing demo
DIRTY_COMMENTS = [
    "This is AMAZING!!! 🔥🔥🔥 Love it!!! ❤️❤️ #{topic}",
    "OMG 😍😍😍 best post ever!!! Check out https://spam.example.com/link for more",
    "I     think     this    is   great!!!     #{topic}   !!!",
    "NULL comment with <script>alert('xss')</script> and special chars: §±×÷",
    "Visit http://malicious-link.example.com for more info on {topic} 🎉🎉🎉",
    "Great post! ✨✨✨ {topic} is the future!! 🚀🚀🚀 www.example.com/spam",
    "BUY CRYPTO NOW!!! 💰💰💰 {topic} is the next big thing!! http://scam.example.com",
    "follow me on instagram @fake_account for more {topic} content 📸📸📸",
    "FIRST!!! 🏆🏆🏆",
    "nobody asked 💀💀💀",
    "",  # Empty comment (should be filtered)
    "   ",  # Whitespace only (should be filtered)
    "  \t  \n  ",  # More whitespace variations
    "a",  # Very short comment
    "ok",  # Minimal comment
]

# Real public image URLs from Picsum (Lorem Picsum) - these actually load
# Using seed-based URLs for consistency
REAL_IMAGE_URLS = [
    "https://picsum.photos/seed/techdiagram/800/600",
    "https://picsum.photos/seed/serverarch/800/600",
    "https://picsum.photos/seed/dashboard1/800/600",
    "https://picsum.photos/seed/codesnippet/800/600",
    "https://picsum.photos/seed/networkmap/800/600",
    "https://picsum.photos/seed/cloudinfra/800/600",
    "https://picsum.photos/seed/devsetup/800/600",
    "https://picsum.photos/seed/benchmark1/800/600",
    "https://picsum.photos/seed/grafana1/800/600",
    "https://picsum.photos/seed/kubernetes/800/600",
    "https://picsum.photos/seed/architecture/800/600",
    "https://picsum.photos/seed/monitoring/800/600",
    "https://picsum.photos/seed/pipeline1/800/600",
    "https://picsum.photos/seed/database1/800/600",
    "https://picsum.photos/seed/microservic/800/600",
    "https://picsum.photos/seed/dataflow1/800/600",
    "https://picsum.photos/seed/sparkjob1/800/600",
    "https://picsum.photos/seed/kafkatopic/800/600",
    "https://picsum.photos/seed/hdfsnodes/800/600",
    "https://picsum.photos/seed/dockercomp/800/600",
    "https://picsum.photos/seed/cicdpipe/800/600",
    "https://picsum.photos/seed/loadtest1/800/600",
    "https://picsum.photos/seed/terminal1/800/600",
    "https://picsum.photos/seed/whiteboard/800/600",
    "https://picsum.photos/seed/meeting1/800/600",
    "https://picsum.photos/seed/laptop1/800/600",
    "https://picsum.photos/seed/office1/800/600",
    "https://picsum.photos/seed/hackathon/800/600",
    "https://picsum.photos/seed/conference/800/600",
    "https://picsum.photos/seed/coding1/800/600",
]

IMAGE_CAPTIONS = [
    "Architecture diagram of our {topic} system",
    "Benchmark results comparing {topic} approaches",
    "Grafana dashboard showing {topic} metrics",
    "Network topology for our {topic} deployment",
    "Screenshot of our {topic} monitoring setup",
    "Whiteboard sketch of the {topic} design",
    "Performance graph after implementing {topic}",
    "Our team's {topic} setup at the hackathon",
    "Load test results for {topic} under 100k concurrent users",
    "Before and after comparison of {topic} migration",
    "Terminal output showing {topic} processing logs",
    "System diagram with {topic} integration points",
    "Resource utilization chart for {topic} cluster",
    "Error rate dashboard after deploying {topic}",
    "Latency heatmap for {topic} service calls",
]


def generate_comment(post_topic: str) -> dict:
    """Generate a single comment."""
    topic = random.choice(POST_TOPICS)

    # 80% normal comments, 20% dirty comments (for preprocessing demo)
    if random.random() < 0.8:
        text = random.choice(COMMENT_BODIES).format(topic=topic)
    else:
        text = random.choice(DIRTY_COMMENTS).format(topic=topic)

    return {
        "commentId": str(uuid.uuid4()),
        "userId": f"user_{random.randint(1, 2000)}",
        "username": random.choice(USERNAMES),
        "commentText": text,
        "timestamp": int(time.time() * 1000)
        - random.randint(0, 86400000 * 7),  # Last 7 days
        "upvotes": random.randint(-20, 1000),
        "parentCommentId": str(uuid.uuid4()) if random.random() > 0.65 else None,
    }


def generate_post(num_comments: int) -> dict:
    """Generate a post with nested comments."""
    topic = random.choice(POST_TOPICS)
    subreddit = random.choice(SUBREDDITS)

    # ~35% of posts have images (with real URLs)
    has_image = random.random() < 0.35

    post = {
        "postId": str(uuid.uuid4()),
        "userId": f"user_{random.randint(1, 1000)}",
        "username": random.choice(USERNAMES),
        "title": random.choice(POST_TITLE_TEMPLATES).format(
            topic=topic, subreddit=subreddit
        ),
        "postText": random.choice(POST_BODY_TEMPLATES).format(topic=topic),
        "subreddit": subreddit,
        "imageUrl": random.choice(REAL_IMAGE_URLS) if has_image else None,
        "imageCaption": random.choice(IMAGE_CAPTIONS).format(topic=topic)
        if has_image
        else None,
        "timestamp": int(time.time() * 1000)
        - random.randint(0, 604800000),  # Last 7 days
        "upvotes": random.randint(0, 15000),
        "comments": [generate_comment(topic) for _ in range(num_comments)],
    }

    # ~10% chance of duplicate comments (for dedup testing)
    if post["comments"] and random.random() > 0.9:
        dup = post["comments"][0].copy()
        post["comments"].append(dup)

    # ~5% chance of null postText (for validation testing)
    if random.random() > 0.95:
        post["postText"] = None

    return post


def generate_bulk_data(total_comments: int, output_file: str):
    """Generate bulk JSON data with specified total comment count."""
    print(f"\n{'=' * 60}")
    print(f"  BULK DATA GENERATOR (Enhanced for Demo)")
    print(f"  Target: {total_comments:,} total comments")
    print(f"  Output: {output_file}")
    print(f"{'=' * 60}\n")

    posts = []
    total_generated = 0
    post_count = 0
    image_posts = 0

    start_time = time.time()

    while total_generated < total_comments:
        remaining = total_comments - total_generated
        # Random number of comments per post (3-20)
        num_comments = min(random.randint(3, 20), remaining)

        post = generate_post(num_comments)
        posts.append(post)
        total_generated += num_comments
        post_count += 1
        if post.get("imageUrl"):
            image_posts += 1

        # Progress every 50 posts
        if post_count % 50 == 0:
            pct = (total_generated / total_comments) * 100
            sys.stdout.write(
                f"\r  Progress: {total_generated:,}/{total_comments:,} comments ({pct:.0f}%) in {post_count} posts"
            )
            sys.stdout.flush()

    elapsed = time.time() - start_time

    # Write to file
    print(f"\n\n  Writing to {output_file}...")
    with open(output_file, "w") as f:
        json.dump(posts, f, indent=None)  # No indent for smaller file size

    # File size
    import os

    file_size_bytes = os.path.getsize(output_file)
    if file_size_bytes > 1024 * 1024:
        file_size_str = f"{file_size_bytes / (1024 * 1024):.1f} MB"
    else:
        file_size_str = f"{file_size_bytes / 1024:.1f} KB"

    # Data quality stats
    dirty_count = sum(
        1
        for post in posts
        for comment in post.get("comments", [])
        if any(
            c in (comment.get("commentText") or "")
            for c in ["🔥", "😍", "<script>", "http://", "NULL", "🚀", "💰", "💀"]
        )
    )
    empty_count = sum(
        1
        for post in posts
        for comment in post.get("comments", [])
        if not (comment.get("commentText") or "").strip()
    )
    null_posts = sum(1 for p in posts if p.get("postText") is None)
    dup_comments = sum(
        1 for p in posts for c in p.get("comments", []) if p["comments"].count(c) > 1
    )

    print(f"\n{'=' * 60}")
    print(f"  GENERATION COMPLETE")
    print(f"{'=' * 60}")
    print(f"  Posts:            {post_count:,}")
    print(
        f"  Posts with images:{image_posts:,} ({image_posts / post_count * 100:.0f}%)"
    )
    print(f"  Total comments:   {total_generated:,}")
    print(f"  File size:        {file_size_str}")
    print(f"  Generation time:  {elapsed:.1f}s")
    print(f"{'=' * 60}")
    print(f"  DATA QUALITY BREAKDOWN (for preprocessing demo):")
    print(f"  Clean comments:     {total_generated - dirty_count - empty_count:,}")
    print(
        f"  Dirty/spam:         {dirty_count:,} ({dirty_count / max(total_generated, 1) * 100:.1f}%)"
    )
    print(f"  Empty/whitespace:   {empty_count:,}")
    print(f"  Null post text:     {null_posts:,}")
    print(f"  Duplicate comments: {dup_comments:,}")
    print(f"{'=' * 60}")

    # Estimated processing time
    clean_count = total_generated - empty_count
    est_minutes = clean_count / 28  # ~28 RPM rate limit
    print(f"\n  ESTIMATED PROCESSING TIME:")
    print(f"  At ~28 requests/min (Groq free tier): ~{est_minutes:.0f} minutes")
    print(f"  At ~60 requests/min (Groq developer):  ~{clean_count / 60:.0f} minutes")
    print(f"{'=' * 60}\n")


def main():
    parser = argparse.ArgumentParser(
        description="Generate bulk JSON data for batch processing demo"
    )
    parser.add_argument(
        "--count",
        type=int,
        default=5000,
        help="Total number of comments to generate (default: 5000)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="demo_bulk_data.json",
        help="Output file path (default: demo_bulk_data.json)",
    )
    args = parser.parse_args()

    generate_bulk_data(args.count, args.output)


if __name__ == "__main__":
    main()
