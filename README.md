# Reddit AI Reply Generator

A production-grade **Big Data Analytics system** that automatically generates AI-powered replies to Reddit comments in real-time. Built on a full big data stack with both streaming and batch processing capabilities.

---

## 🏗️ Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    FRONTEND (React + TS)                     │
│                 Nginx reverse proxy + Vite                   │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                   API GATEWAY (Python)                       │
│    FastAPI - Request routing, HDFS access, SQLite DB        │
└─────┬─────────────────┬─────────────────┬───────────────────┘
      │                 │                 │
      ▼                 ▼                 ▼
┌──────────┐    ┌──────────────┐   ┌──────────────────┐
│  KAFKA   │    │  INFERENCE   │   │  SPARK BATCH     │
│(Streaming)│   │  SERVICE     │   │  (Scala)         │
└────┬─────┘    │  (Python)    │   │  HTTP endpoint   │
     │          │  - Groq API  │   └──────────────────┘
     │          │  - Multi-key │
     │          │  - Batching  │
     ▼          └──────────────┘
┌──────────────────────────────────┐
│  SPARK STREAMING (Scala)         │
│  Kafka → Preprocessing → HDFS    │
└──────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────┐
│          HADOOP HDFS                │
│  Distributed storage for all data   │
└─────────────────────────────────────┘
```

---

## ✨ Features

- **Real-time streaming** — Process live Reddit comments via Kafka with ~10–15s end-to-end latency
- **Batch processing** — Handle 50,000+ comments with progress tracking
- **LLM-powered replies** — Contextual AI replies using Groq (`llama-3.1-8b-instant`) or OpenAI
- **Multi-comment batching** — 10x reduction in API calls by packing multiple comments per LLM request
- **Multi-key API pooling** — Round-robin across multiple Groq keys for 3× throughput
- **Fault tolerance** — Spark checkpointing, retries, and graceful degradation
- **Full observability** — Spark UI, HDFS UI, Kafka UI, job progress tracking

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Frontend | React 18, TypeScript, Vite, Nginx |
| API Gateway | Python, FastAPI, aiokafka |
| Stream Processing | Apache Spark Structured Streaming (Scala) |
| Batch Processing | Apache Spark (Scala), HTTP job server |
| Message Queue | Apache Kafka (6 partitions per topic) |
| Distributed Storage | Apache Hadoop HDFS |
| Inference | Python, Groq API / OpenAI API |
| Containerization | Docker, Docker Compose |

---

## 📁 Project Structure

```
.
├── frontend/                  # React + TypeScript UI
├── api-gateway/               # FastAPI orchestration layer
├── inference-service/         # LLM reply generation service
│   └── app/
│       └── main.py
├── spark-streaming/           # Spark Structured Streaming (Scala)
│   └── src/
│       ├── StreamingApp.scala
│       └── PreprocessingEngine.scala
├── spark-batch/               # Spark Batch processing (Scala)
│   └── src/
│       └── BatchApp.scala
├── docker-compose.yml
└── scripts/
    └── generate_bulk_data.py
```

---

## 🚀 Getting Started

### Prerequisites

- Docker & Docker Compose
- Groq API key(s) — [get one free at console.groq.com](https://console.groq.com)

### 1. Clone the repository

```bash
git clone https://github.com/your-username/reddit-ai-reply-generator.git
cd reddit-ai-reply-generator
```

### 2. Configure environment variables

```bash
cp .env.example .env
```

Edit `.env`:

```env
LLM_PROVIDER=groq
GROQ_API_KEYS=your_key_1,your_key_2,your_key_3
```

### 3. Start all services

```bash
docker-compose up --build
```

### 4. Access the UI

| Service | URL |
|---|---|
| Frontend | http://localhost:3000 |
| API Gateway | http://localhost:8001 |
| Spark Master UI | http://localhost:8080 |
| Spark Driver UI | http://localhost:4040 |
| HDFS NameNode UI | http://localhost:9870 |
| Spark Batch Service | http://localhost:8085 |

---

## 🔄 Data Flow

### Streaming Mode

```
User submits comment
  → API Gateway → Kafka (comments_topic)
  → Spark Streaming reads micro-batch (every 5s)
  → Preprocessing: clean, validate, deduplicate (Scala)
  → Write cleaned data to HDFS
  → Batch comments by post → Inference Service
  → LLM generates replies (batched, multi-key)
  → Write replies to HDFS + Kafka (replies_topic)
  → Frontend displays reply via WebSocket
```

### Batch Mode

```
User uploads JSON file (up to 50k comments)
  → API Gateway writes to HDFS (/data/uploads)
  → Triggers Spark Batch job via HTTP
  → Spark reads + preprocesses data
  → Intelligent repartition (totalRecords / batchSize)
  → Per-partition inference with progress tracking
  → Results joined and written to HDFS
  → User downloads enriched JSON results
```

---

## ⚡ Key Optimizations

### Multi-Comment Batching (10× speedup)

Instead of one API call per comment, up to 10 comments are packed into a single LLM request:

```
Without batching: 50 comments → 50 API calls → ~107s
With batching:    50 comments →  5 API calls → ~11s
```

### Multi-Key API Pooling (3× throughput)

Round-robin across multiple Groq API keys circumvents per-key rate limits:

```
1 key  = 28 RPM
3 keys = 84 RPM  ← 3× throughput
```

### Broadcast Join Optimization

Metadata is collected to the driver once and broadcast to all Spark executors, eliminating costly distributed shuffles inside `mapPartitions`.

### Dynamic Repartitioning

Batch jobs repartition based on `totalRecords / batchSize` to ensure even workload distribution and granular progress updates.

---

## 📊 HDFS Directory Structure

```
/data/
├── raw/
│   ├── streaming/       # Raw streaming data
│   └── bulk/            # Raw bulk uploads
├── cleaned/
│   ├── streaming/       # Preprocessed stream data
│   └── bulk/            # Preprocessed bulk data
├── replies/
│   ├── streaming/       # Generated replies (stream)
│   └── bulk/            # Generated replies (batch)
└── uploads/             # User file uploads

/checkpoints/
└── streaming/           # Spark fault-tolerance checkpoints
```

---

## 🧪 Running the Batch Demo

Generate and process a sample bulk dataset:

```bash
# Generate 1,000 sample comments
python scripts/generate_bulk_data.py --count 1000 --output test.json

# Upload and process via the frontend Batch tab, or via curl:
curl -X POST http://localhost:8001/api/batch/upload -F "file=@test.json"
curl -X POST http://localhost:8001/api/batch/process -d '{"input_path": "/data/uploads/test.json", "output_path": "/data/replies/bulk/test_output"}'

# Check status
curl http://localhost:8001/api/batch/status/{jobId}
```

---

## 📈 Performance Benchmarks

| Mode | Volume | Time | Throughput |
|---|---|---|---|
| Streaming | continuous | ~10–15s latency | ~100 comments/min |
| Batch | 50,000 comments | ~5–10 minutes | ~100–150 comments/sec |
| Inference (single) | 1 comment | ~300–500ms | — |
| Inference (batched) | 10 comments | ~800–1200ms | ~80–120ms/comment |

---

## 🔧 Configuration Reference

### Inference Service

| Variable | Default | Description |
|---|---|---|
| `LLM_PROVIDER` | `groq` | `groq`, `openai`, or `mock` |
| `GROQ_API_KEYS` | — | Comma-separated list of keys |
| `COMMENTS_PER_LLM_CALL` | `10` | Comments packed per API call |
| `MAX_CONCURRENT_REQUESTS` | `15` | Parallel inference requests |
| `RATE_LIMIT_RPM` | `28` | Rate limit per key |

### Spark Streaming

| Variable | Default | Description |
|---|---|---|
| `BATCH_SIZE` | `50` | Comments per inference HTTP call |
| `TRIGGER_INTERVAL` | `5s` | Micro-batch interval |
| `CHECKPOINT_DIR` | `hdfs://.../checkpoints/streaming` | Fault-tolerance checkpoint path |
| `spark.sql.shuffle.partitions` | `6` | Matches Kafka partition count |

---

## 🌐 API Reference

### API Gateway Endpoints

```
POST /api/stream/post           Submit a post to Kafka
POST /api/stream/comment        Submit a comment to Kafka
POST /api/batch/upload          Upload bulk JSON to HDFS
POST /api/batch/process         Trigger Spark batch job
GET  /api/batch/status/{jobId}  Poll job progress
GET  /api/batch/results/{jobId} Download results
GET  /api/hdfs/list/{path}      Browse HDFS directory
WS   /ws                        WebSocket for live reply updates
```

### Spark Batch Service Endpoints

```
POST /process          Trigger batch processing job
GET  /status/{jobId}   Check job progress
GET  /health           Health check
```

### Inference Service Endpoints

```
POST /infer            Single or batched inference
POST /batch-infer      Batch inference (called by Spark)
POST /describe-image   Multimodal image description
GET  /metrics          API key pool stats and throughput
GET  /health           Health check
```

---

## 🔍 Monitoring

- **Spark jobs**: Spark Master UI (`localhost:8080`), Driver UI (`localhost:4040`)
- **Kafka topics & lag**: Kafka UI (if enabled)
- **HDFS health**: NameNode UI (`localhost:9870`)
- **Inference metrics**: `GET /metrics` on inference service returns per-key call counts and error rates

---

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Commit your changes (`git commit -m 'Add my feature'`)
4. Push to the branch (`git push origin feature/my-feature`)
5. Open a Pull Request

---

## 📄 License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
