<<<<<<< HEAD
# RedditAI - Big Data Social Media Reply Generation System

## Real-Time AI-Powered Reply Generation Using Kafka, Spark, HDFS, and LLM

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [System Architecture](#2-system-architecture)
3. [Technology Stack](#3-technology-stack)
4. [Project Directory Structure](#4-project-directory-structure)
5. [Infrastructure Services](#5-infrastructure-services)
   - [Apache Zookeeper](#51-apache-zookeeper)
   - [Apache Kafka](#52-apache-kafka)
   - [HDFS (Hadoop Distributed File System)](#53-hdfs-hadoop-distributed-file-system)
   - [Apache Spark Cluster](#54-apache-spark-cluster)
6. [Application Services](#6-application-services)
   - [API Gateway (Python/FastAPI)](#61-api-gateway-pythonfastapi)
   - [Inference Service (Python/FastAPI)](#62-inference-service-pythonfastapi)
   - [Spark Streaming Service (Scala)](#63-spark-streaming-service-scala)
   - [Spark Batch Service (Scala)](#64-spark-batch-service-scala)
   - [Frontend (React/TypeScript/Nginx)](#65-frontend-reacttypescriptnginx)
   - [Ngrok Tunneling](#66-ngrok-tunneling)
7. [Data Flow: Streaming Pipeline](#7-data-flow-streaming-pipeline)
8. [Data Flow: Batch Pipeline](#8-data-flow-batch-pipeline)
9. [Preprocessing Pipelines](#9-preprocessing-pipelines)
   - [Streaming Preprocessing (6 Steps)](#91-streaming-preprocessing-6-steps)
   - [Batch Preprocessing (8 Steps)](#92-batch-preprocessing-8-steps)
10. [LLM Inference Strategy](#10-llm-inference-strategy)
    - [Multi-Key API Pooling](#101-multi-key-api-pooling)
    - [Rate Limiting](#102-rate-limiting)
    - [Multi-Comment Batching](#103-multi-comment-batching)
    - [Prompt Engineering](#104-prompt-engineering)
    - [Image Captioning](#105-image-captioning)
11. [Script Files (Line-by-Line)](#11-script-files-line-by-line)
    - [generate_bulk_data.py](#111-generate_bulk_datapy)
    - [simulate_velocity.py](#112-simulate_velocitypy)
    - [start-ngrok.bat / start-ngrok.sh](#113-start-ngrokbat--start-ngroksh)
12. [Configuration Details](#12-configuration-details)
    - [Docker Compose Services](#121-docker-compose-services)
    - [Kafka Configuration](#122-kafka-configuration)
    - [HDFS Configuration](#123-hdfs-configuration)
    - [Spark Configuration](#124-spark-configuration)
    - [Environment Variables](#125-environment-variables)
13. [Data Transformation at Each Stage](#13-data-transformation-at-each-stage)
14. [How to Run the Project](#14-how-to-run-the-project)
15. [UI Ports and Dashboards](#15-ui-ports-and-dashboards)

---

## 1. Project Overview

This project is a full-stack distributed platform that replicates a Reddit-like social media experience with an integrated big data pipeline. The system automatically generates AI-powered replies to user comments in real-time using large language models (LLMs).

The platform supports two distinct processing pipelines:

- **Real-Time Streaming Pipeline**: When a user posts a comment on the frontend, the comment flows through Kafka into Spark Structured Streaming, gets preprocessed, sent to an LLM inference service, and the generated AI reply is delivered back to the user's browser via WebSocket - all within seconds.

- **Batch Processing Pipeline**: Users can upload a JSON file containing thousands of posts and comments. The file is stored in HDFS, processed by a Spark batch job with full preprocessing and LLM inference, and the results (original data enriched with AI replies) are made available for download as a JSON file.

The entire system runs as 12+ Docker containers orchestrated via Docker Compose, making it fully reproducible and deployable on any machine with Docker installed.

### What Makes This Project Unique

- **End-to-end big data pipeline**: Data flows from user input through message queuing (Kafka), distributed processing (Spark), AI inference (Groq/LLaMA), and back to the user interface - demonstrating a complete big data architecture
- **Two processing paradigms**: Both real-time streaming and batch processing are implemented side by side, showcasing the Lambda architecture pattern
- **Production-grade rate limiting**: A custom multi-key API pool with per-key sliding-window rate limiters allows the system to sustain 140 requests per minute to the LLM API
- **Multi-comment batching**: Up to 10 comments are packed into a single LLM call, reducing API usage by 10x while maintaining reply quality
- **Real-time WebSocket delivery**: AI replies appear under comments in real-time without page refresh
- **Image support with AI captioning**: Users can attach images to posts, which are automatically captioned by a vision LLM model

---

## 2. System Architecture

### High-Level Architecture Diagram

```
+----------------------------------------------------------+
|                     USER'S BROWSER                        |
|  +----------------------------------------------------+  |
|  |          React Frontend (TypeScript)                |  |
|  |  - Login Screen    - Feed View    - Batch View      |  |
|  |  - WebSocket Client for Real-Time Replies           |  |
|  +---------------------------+------------------------+   |
+------------------------------|------------------------+   
                               |                            
                     HTTP / WebSocket                       
                               |                            
+------------------------------|----------------------------+
|                   Nginx Reverse Proxy                     |
|            (frontend container, port 3000)                |
|  Static files: /          -> React SPA                    |
|  API proxy:    /api/*     -> api-gateway:8001             |
|  WS proxy:    /ws/*      -> api-gateway:8001              |
+------------------------------|----------------------------+
                               |
                +--------------|---------------+
                |      API Gateway             |
                |   (FastAPI, port 8001)        |
                |                              |
                |  - REST API (CRUD)           |
                |  - Authentication (JWT)      |
                |  - SQLite Database (WAL)     |
                |  - Kafka Producer            |
                |  - Kafka Consumer (replies)  |
                |  - WebSocket Server          |
                |  - HDFS Client (WebHDFS)     |
                |  - Image Upload + Captioning |
                |  - Seed Data on Startup      |
                +----|---------|----------|-----+
                     |         |          |
          +----------+    +----+----+     +----------+
          |               |         |                |
  +-------v--------+ +---v---+ +---v---------+ +----v----------+
  | Kafka Broker   | | HDFS  | | Inference   | | WebSocket     |
  | (port 9092)    | | Name  | | Service     | | Broadcast     |
  |                | | Node  | | (port 8000) | | to Browser    |
  | Topics:        | | (9870)| |             | +---------------+
  | comments_topic | | +Data | | - Groq API  |
  | posts_topic    | | Node  | | - LLaMA 3.1 |
  | replies_topic  | | (9864)| | - Key Pool  |
  | bulk_topic     | +---+---+ | - Batching  |
  +---|-----|------+     |     | - Captioning|
      |     |            |     +------+------+
      |     |            |            ^
      |     |            |            |  HTTP POST
      |     |            |            |  /generate
+-----v-----v------------v------------+----------+
|              Apache Spark Cluster               |
|  +------------------+  +---------------------+  |
|  | Spark Master     |  | Spark Worker        |  |
|  | (port 8080/7077) |  | (6GB RAM, 4 cores)  |  |
|  +------------------+  +---------------------+  |
|                                                  |
|  +---------------------+  +-------------------+  |
|  | Spark Streaming     |  | Spark Batch       |  |
|  | Service             |  | Service           |  |
|  | (Scala)             |  | (Scala, port 8085)|  |
|  |                     |  |                   |  |
|  | Reads: Kafka        |  | Reads: HDFS       |  |
|  | Writes: HDFS+Kafka  |  | Writes: HDFS      |  |
|  | Calls: Inference    |  | Calls: Inference  |  |
|  +---------------------+  +-------------------+  |
+--------------------------------------------------+

+--------------------------------------------------+
|              Zookeeper (port 2181)                |
|         Kafka Cluster Coordination               |
+--------------------------------------------------+
```

### Streaming Pipeline Flow

```
User Comment ──> API Gateway ──> Kafka (comments_topic)
                                        |
                                        v
                              Spark Structured Streaming
                                        |
                              6-Step Preprocessing
                                        |
                              Write Cleaned ──> HDFS
                                        |
                              Call Inference Service
                                        |
                              LLM (Groq/LLaMA 3.1)
                                        |
                              Write Replies ──> HDFS
                              Publish ──> Kafka (replies_topic)
                                        |
                              API Gateway Consumer
                                        |
                              Save to SQLite
                                        |
                              WebSocket Broadcast
                                        |
                              Browser Shows Reply
```

### Batch Pipeline Flow

```
User Uploads JSON ──> API Gateway ──> HDFS (/data/raw/bulk/)
                                        |
                            Trigger Spark Batch (HTTP POST)
                                        |
                              Spark Batch Job
                                        |
                              Read JSON from HDFS
                                        |
                              8-Step Preprocessing
                                        |
                              Write Cleaned ──> HDFS
                                        |
                              Call Inference Service
                              (with progress tracking)
                                        |
                              LLM (Groq/LLaMA 3.1)
                                        |
                              Write Enriched Results ──> HDFS
                                        |
                              Frontend Polls Status
                                        |
                              User Downloads Results
=======
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
>>>>>>> f4a68f33a87a2a14146825f9d5f4a5a904f4ea9e
```

---

<<<<<<< HEAD
## 3. Technology Stack

| Technology | Version | Purpose |
|---|---|---|
| **Docker** | Latest | Containerization of all services |
| **Docker Compose** | v3.8 | Multi-container orchestration |
| **Apache Kafka** | 7.5.0 (Confluent) | Distributed message broker for event streaming between services |
| **Apache Zookeeper** | 7.5.0 (Confluent) | Distributed coordination service for Kafka cluster management |
| **Apache Spark** | 3.5.0 | Distributed data processing engine for both streaming and batch |
| **Spark Structured Streaming** | 3.5.0 | Real-time micro-batch stream processing on Kafka data |
| **Scala** | 2.12 | Language for Spark streaming and batch processing services |
| **SBT** | 1.9.7 | Scala build tool for compiling Spark services |
| **HDFS (Hadoop)** | 3.2.1 | Distributed file system for storing raw data, cleaned data, and results |
| **Python** | 3.11 | Language for API Gateway and Inference Service |
| **FastAPI** | 0.104+ | Async web framework for API Gateway and Inference Service |
| **Uvicorn** | Latest | ASGI server for running FastAPI applications |
| **SQLite** | Built-in | Lightweight relational database for API Gateway (WAL mode) |
| **React** | 18.3.1 | Frontend UI library for the single-page application |
| **TypeScript** | 5.6.2 | Typed JavaScript for frontend development |
| **Vite** | 6.0.5 | Frontend build tool and dev server |
| **Nginx** | Alpine | Reverse proxy and static file server for the frontend |
| **Groq Cloud API** | Latest | LLM inference API provider (hosts LLaMA models) |
| **LLaMA 3.1 8B Instant** | 3.1 | Large language model for generating text replies |
| **LLaMA 4 Scout 17B** | 4.0 | Vision-capable LLM for image captioning |
| **httpx** | 0.27+ | Async HTTP client for Python (API calls) |
| **aiokafka** | Latest | Async Kafka client for Python (producer/consumer) |
| **websockets** | Latest | WebSocket server implementation for real-time communication |
| **Ngrok** | Latest | Secure tunneling for exposing local services to the internet |
| **Lucide React** | Latest | Icon library used in the frontend |
| **React Hot Toast** | Latest | Toast notification library for frontend |

---

## 4. Project Directory Structure

```
reddit/
|
+-- infrastructure/
|   +-- docker-compose.yml          # All 14 service definitions (428 lines)
|   +-- .env                        # Environment variables (API keys, URLs, tokens)
|   +-- ngrok/
|       +-- ngrok.yml               # Ngrok tunnel configuration
|
+-- services/
|   +-- api-gateway/
|   |   +-- app/
|   |   |   +-- main.py             # Full API Gateway application (1,347 lines)
|   |   +-- Dockerfile              # Python 3.11 slim container
|   |   +-- requirements.txt        # Python dependencies (10 packages)
|   |
|   +-- inference-service/
|   |   +-- app/
|   |   |   +-- main.py             # LLM inference service (1,054 lines)
|   |   +-- Dockerfile              # Python 3.11 slim container
|   |   +-- requirements.txt        # Python dependencies (7 packages)
|   |
|   +-- spark-streaming-scala/
|   |   +-- src/main/scala/com/reddit/streaming/
|   |   |   +-- StreamingApp.scala           # Main streaming application (244 lines)
|   |   |   +-- PreprocessingEngine.scala    # 6-step preprocessing pipeline (207 lines)
|   |   |   +-- InferenceClient.scala        # HTTP client for inference service (130 lines)
|   |   |   +-- Models.scala                 # Case classes / data models (96 lines)
|   |   +-- build.sbt               # SBT build configuration (26 lines)
|   |   +-- Dockerfile              # Multi-stage SBT build (46 lines)
|   |
|   +-- spark-batch-scala/
|   |   +-- src/main/scala/com/reddit/batch/
|   |   |   +-- BatchApp.scala                  # Main batch application with HTTP server (322 lines)
|   |   |   +-- BatchPreprocessingEngine.scala   # 8-step preprocessing pipeline (236 lines)
|   |   |   +-- BatchInferenceClient.scala       # HTTP client for inference (123 lines)
|   |   |   +-- Models.scala                    # Case classes / data models (84 lines)
|   |   +-- build.sbt               # SBT build configuration (26 lines)
|   |   +-- Dockerfile              # Multi-stage SBT build (43 lines)
|   |
|   +-- frontend/
|       +-- src/
|       |   +-- App.tsx              # Main React application (1,078 lines)
|       |   +-- index.css            # Dark theme styling (1,368 lines)
|       |   +-- main.tsx             # React entry point (10 lines)
|       +-- nginx.conf              # Nginx reverse proxy config (47 lines)
|       +-- Dockerfile              # Multi-stage node -> nginx build (27 lines)
|       +-- package.json            # NPM dependencies (22 lines)
|       +-- vite.config.ts          # Vite build configuration (20 lines)
|
+-- scripts/
|   +-- generate_bulk_data.py       # Bulk JSON data generator (481 lines)
|   +-- simulate_velocity.py        # Async velocity/load simulator (238 lines)
|   +-- start-ngrok.bat             # Windows ngrok launcher (87 lines)
|   +-- start-ngrok.sh              # Linux/Mac ngrok launcher (94 lines)
|
+-- .env                            # Root environment file (34 lines)
+-- README.md                       # This file
=======
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
>>>>>>> f4a68f33a87a2a14146825f9d5f4a5a904f4ea9e
```

---

<<<<<<< HEAD
## 5. Infrastructure Services

### 5.1 Apache Zookeeper

**Image**: `confluentinc/cp-zookeeper:7.5.0`  
**Port**: 2181  
**Role**: Distributed coordination service

Zookeeper is a centralized service for maintaining configuration information, naming, and providing distributed synchronization. In this project, Zookeeper serves as the coordination backbone for the Kafka broker.

**What Zookeeper does in this system**:
- Manages Kafka broker registration and discovery
- Maintains Kafka topic metadata (partition assignments, leader election)
- Handles Kafka broker health monitoring
- Coordinates Kafka consumer group membership

**Configuration**:
```
ZOOKEEPER_CLIENT_PORT=2181        # Port clients connect on
ZOOKEEPER_TICK_TIME=2000          # Base time unit in milliseconds (heartbeat interval)
```

The `TICK_TIME` of 2000ms means Zookeeper sends heartbeats every 2 seconds. If a Kafka broker misses too many heartbeats (default: 10 ticks = 20 seconds), Zookeeper considers it dead and triggers leader re-election for its partitions.

**Health Check**: The docker-compose configuration includes a health check that runs `echo ruok | nc localhost 2181` every 10 seconds. The command sends the "are you ok?" command to Zookeeper; a healthy Zookeeper responds with "imok".

---

### 5.2 Apache Kafka

**Image**: `confluentinc/cp-kafka:7.5.0`  
**Ports**: 9092 (internal Docker network), 29092 (host machine access)  
**Role**: Distributed event streaming platform / message broker

Kafka is the central nervous system of this project. Every event (new comment, new post, AI reply) flows through Kafka topics. It decouples the API Gateway from the Spark processing layer, enabling asynchronous, fault-tolerant communication.

**Why Kafka is used**:
- **Decoupling**: The API Gateway does not need to know about Spark; it simply publishes messages to topics
- **Buffering**: If Spark is temporarily slower than the rate of incoming comments, Kafka buffers the messages
- **Durability**: Messages are persisted to disk with a retention period of 7 days (168 hours)
- **Parallelism**: Each topic has multiple partitions, allowing parallel consumption by Spark executors
- **Replay**: Messages can be re-read from any offset, enabling fault recovery

**Kafka Topics** (created by the `kafka-init` init container):

| Topic | Partitions | Retention | Purpose |
|---|---|---|---|
| `comments_topic` | 6 | 7 days | Carries user comments enriched with post context from API Gateway to Spark Streaming |
| `posts_topic` | 6 | 7 days | Carries newly created posts (reserved for future use) |
| `replies_topic` | 6 | 7 days | Carries AI-generated replies from Spark back to API Gateway |
| `bulk_topic` | 3 | 7 days | Reserved for bulk operation notifications |

**Key Kafka Configuration**:
```
KAFKA_BROKER_ID=1                                    # Unique broker identifier
KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181               # Zookeeper connection string
KAFKA_ADVERTISED_LISTENERS=                          # Two listener endpoints:
  PLAINTEXT://kafka:9092,                            #   Internal Docker network
  PLAINTEXT_HOST://localhost:29092                    #   Host machine access
KAFKA_NUM_PARTITIONS=6                               # Default partitions for auto-created topics
KAFKA_LOG_RETENTION_HOURS=168                        # 7 days message retention
KAFKA_LOG_RETENTION_BYTES=1073741824                 # 1 GB max retention per partition
KAFKA_AUTO_CREATE_TOPICS_ENABLE=true                 # Allow topics to be created on first use
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1              # Single broker, so replication = 1
```

**Topic Initialization** (kafka-init container):
The `kafka-init` service is an init container that runs once at startup and exits. It uses `kafka-topics --create --if-not-exists` to create each topic, ensuring idempotent setup. The container depends on the Kafka broker being healthy before running.

**How Kafka partitions enable parallelism**:
With 6 partitions on `comments_topic`, Spark Streaming can assign up to 6 concurrent tasks to read from Kafka in parallel. Each Spark executor reads from a subset of partitions. The `comments_topic` uses Kafka's default partitioner, which distributes messages across partitions based on a hash of the message key (or round-robin if no key is specified).

---

### 5.3 HDFS (Hadoop Distributed File System)

**Images**: `bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8` and `bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8`  
**Ports**: 9870 (NameNode Web UI), 9000 (NameNode RPC), 9864 (DataNode)  
**Role**: Distributed file storage for raw data, cleaned data, and processed results

HDFS provides the persistent storage layer for the big data pipeline. All intermediate and final data products are stored in HDFS, enabling Spark to read and write data in a distributed manner.

**HDFS Architecture in this project**:

```
+-------------------+          +-------------------+
|    NameNode       |          |    DataNode       |
|    (port 9870)    | <------> |    (port 9864)    |
|                   |          |                   |
|  - File metadata  |          |  - Actual data    |
|  - Block mapping  |          |    blocks         |
|  - Namespace      |          |  - Block reports  |
+-------------------+          +-------------------+
```

- **NameNode**: Stores file system metadata (directory tree, file-to-block mapping, permissions). Does NOT store actual data. Acts as the "index" of the file system.
- **DataNode**: Stores actual data blocks. Reports block health to NameNode periodically. In this project, only one DataNode is used (development setup).

**HDFS Directory Structure** (created by `hdfs-init` container):

```
/data/
  +-- raw/
  |   +-- streaming/       # Raw data from Kafka (before preprocessing)
  |   +-- bulk/            # Uploaded bulk JSON files
  +-- cleaned/
  |   +-- streaming/       # Preprocessed streaming data
  |   +-- bulk/            # Preprocessed bulk data
  +-- replies/
  |   +-- streaming/       # AI replies from streaming pipeline
  |   +-- bulk/            # AI replies from batch pipeline
  +-- uploads/             # General uploaded files
/checkpoints/
  +-- streaming/           # Spark Structured Streaming checkpoints
```

**Configuration** (via Hadoop XML environment variables):
```
CORE_CONF_fs_defaultFS=hdfs://hadoop-namenode:9000        # Default filesystem URI
CORE_CONF_hadoop_http_staticuser_user=root                # WebHDFS user
HDFS_CONF_dfs_replication=1                               # Single replica (dev setup)
HDFS_CONF_dfs_permissions_enabled=false                   # No permission checks
HDFS_CONF_dfs_webhdfs_enabled=true                        # Enable WebHDFS REST API
HDFS_CONF_dfs_namenode_datanode_registration_ip___hostname___check=false
```

**WebHDFS REST API**: The API Gateway communicates with HDFS using the WebHDFS REST API (not the native Hadoop client). This is a two-step process for file uploads:
1. `PUT /webhdfs/v1/<path>?op=CREATE` returns a redirect URL to the DataNode
2. `PUT <redirect-url>` sends the actual file data to the DataNode

This two-step redirect is a standard WebHDFS protocol design - the NameNode decides which DataNode should store the data, then redirects the client to write directly to that DataNode.

**Why HDFS is used instead of local storage**:
- Spark executors may run on different machines; HDFS provides a shared filesystem accessible by all
- Spark Structured Streaming requires a checkpoint location on a reliable filesystem; HDFS serves this purpose
- HDFS enables the storage of large datasets that exceed the capacity of a single machine
- In a production setup, HDFS would provide replication for fault tolerance

---

### 5.4 Apache Spark Cluster

**Image**: `apache/spark:3.5.0`  
**Ports**: 8080 (Master Web UI), 7077 (Master RPC), 4040 (Application UI), 8081 (Worker Web UI)  
**Role**: Distributed data processing engine

The Spark cluster consists of a master node and a worker node. Two custom Spark applications (streaming and batch) submit their jobs to this cluster.

**Spark Master** (`spark-master`):
- Runs the Spark standalone cluster manager
- Listens on port 7077 for worker registration and job submissions
- Provides a web UI on port 8080 showing cluster status, running applications, and completed jobs
- Allocates resources (cores, memory) to submitted applications

**Spark Worker** (`spark-worker`):
- Registers with the master at `spark://spark-master:7077`
- Provides 6 GB of memory and 4 CPU cores to the cluster
- Executes tasks assigned by the master
- Reports resource usage and task completion back to the master

**How Spark applications connect**:
Both the streaming and batch Scala applications create a `SparkSession` configured with:
```scala
SparkSession.builder()
  .appName("ApplicationName")
  .master("spark://spark-master:7077")   // Connect to the cluster master
  .config("spark.driver.memory", "1g")    // Driver memory allocation
  .config("spark.executor.memory", "1g")  // Executor memory allocation
  .config("spark.cores.max", "2")         // Maximum cores to use
  .getOrCreate()
=======
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
>>>>>>> f4a68f33a87a2a14146825f9d5f4a5a904f4ea9e
```

---

<<<<<<< HEAD
## 6. Application Services

### 6.1 API Gateway (Python/FastAPI)

**File**: `services/api-gateway/app/main.py` (1,347 lines)  
**Port**: 8001  
**Framework**: FastAPI with Uvicorn (1 worker)  
**Database**: SQLite with WAL (Write-Ahead Logging) mode  
**Role**: Central REST API, authentication, Kafka producer/consumer, WebSocket server, HDFS client

The API Gateway is the central hub of the application. Every request from the frontend passes through this service. It handles user management, CRUD operations for posts and comments, publishes events to Kafka, consumes AI replies from Kafka, broadcasts replies via WebSocket, manages bulk upload workflows, and handles image uploads with AI captioning.

#### 6.1.1 Database Schema (SQLite)

The API Gateway initializes five tables on startup:

**`users` table**:
```sql
CREATE TABLE users (
    id TEXT PRIMARY KEY,             -- UUID4 string
    username TEXT UNIQUE NOT NULL,   -- Unique username
    email TEXT,                      -- Optional email
    password_hash TEXT NOT NULL,     -- SHA-256 hashed password
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
```

**`posts` table**:
```sql
CREATE TABLE posts (
    id TEXT PRIMARY KEY,             -- UUID4 string
    title TEXT NOT NULL,             -- Post title
    body TEXT,                       -- Post body text
    image_url TEXT,                  -- Optional image URL
    image_caption TEXT,              -- AI-generated image caption
    user_id TEXT NOT NULL,           -- Foreign key to users.id
    username TEXT NOT NULL,          -- Denormalized username
    subreddit TEXT DEFAULT 'general', -- Subreddit name
    upvotes INTEGER DEFAULT 0,      -- Vote count
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id)
)
```

**`comments` table**:
```sql
CREATE TABLE comments (
    id TEXT PRIMARY KEY,             -- UUID4 string
    post_id TEXT NOT NULL,           -- Foreign key to posts.id
    user_id TEXT NOT NULL,           -- Foreign key to users.id
    username TEXT NOT NULL,          -- Denormalized username
    text TEXT NOT NULL,              -- Comment text
    parent_id TEXT,                  -- Parent comment ID (for nested replies)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (post_id) REFERENCES posts(id),
    FOREIGN KEY (user_id) REFERENCES users(id)
)
```

**`ai_replies` table**:
```sql
CREATE TABLE ai_replies (
    id TEXT PRIMARY KEY,             -- UUID4 string
    comment_id TEXT NOT NULL,        -- Foreign key to comments.id
    post_id TEXT NOT NULL,           -- Foreign key to posts.id
    reply_text TEXT NOT NULL,        -- AI-generated reply text
    model TEXT,                      -- LLM model name used
    generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (comment_id) REFERENCES comments(id)
)
```

**`batch_jobs` table**:
```sql
CREATE TABLE batch_jobs (
    id TEXT PRIMARY KEY,             -- UUID4 string
    filename TEXT NOT NULL,          -- Original upload filename
    hdfs_input_path TEXT,            -- HDFS path to uploaded file
    hdfs_output_path TEXT,           -- HDFS path to results
    status TEXT DEFAULT 'pending',   -- pending/processing/completed/failed
    total_records INTEGER DEFAULT 0, -- Total records to process
    processed_records INTEGER DEFAULT 0, -- Records processed so far
    error_message TEXT,              -- Error details if failed
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP           -- When processing finished
)
```

#### 6.1.2 Authentication System

The API Gateway implements a simple JWT-based authentication system:

- **Registration** (`POST /api/auth/register`): Takes username, email, and password. Hashes the password with SHA-256 (using `hashlib.sha256`). Generates a UUID4 as user ID. Stores in SQLite. Returns a JWT token.
- **Login** (`POST /api/auth/login`): Validates username and password hash. Returns a JWT token containing user_id, username, and expiration (24 hours).
- **Token Verification**: Every authenticated endpoint extracts the `Authorization: Bearer <token>` header, decodes the JWT with `PyJWT`, and validates the expiration.
- **Secret Key**: Uses `JWT_SECRET` environment variable (defaults to `"super-secret-key-change-in-production"`).

#### 6.1.3 REST API Endpoints

| Method | Endpoint | Auth | Description |
|---|---|---|---|
| POST | `/api/auth/register` | No | Register new user |
| POST | `/api/auth/login` | No | Login and get JWT |
| GET | `/api/auth/me` | Yes | Get current user info |
| GET | `/api/posts` | No | Get all posts (newest first) |
| GET | `/api/posts/{post_id}` | No | Get single post with comments and replies |
| POST | `/api/posts` | Yes | Create new post (optional image) |
| POST | `/api/posts/{post_id}/vote` | Yes | Upvote/downvote a post |
| GET | `/api/posts/{post_id}/comments` | No | Get comments for a post |
| POST | `/api/comments` | Yes | Create a comment on a post |
| POST | `/api/upload-image` | Yes | Upload and caption an image |
| POST | `/api/bulk/upload` | Yes | Upload bulk JSON for batch processing |
| GET | `/api/bulk/jobs` | Yes | List user's batch jobs |
| GET | `/api/bulk/status/{job_id}` | Yes | Get batch job status (proxied to Spark) |
| GET | `/api/bulk/download/{job_id}` | Yes | Download batch results from HDFS |
| WebSocket | `/ws/{user_id}` | No | Real-time AI reply delivery |

#### 6.1.4 Kafka Producer

When a user creates a comment, the API Gateway publishes a message to `comments_topic`:

```python
message = {
    "commentId": comment_id,
    "postId": post_id,
    "userId": user_id,
    "username": username,
    "commentText": text,
    "postText": post["body"] or "",       # Enriched with post context
    "postTitle": post["title"],           # Enriched with post title
    "imageUrl": post.get("image_url", ""),
    "imageCaption": post.get("image_caption", ""),
    "parentId": parent_id,
    "timestamp": current_timestamp
}
```

The comment is **enriched with post context** before publishing. This is important because when Spark processes the comment, it needs the post's body text and title to generate a contextually relevant AI reply. Without this enrichment, Spark would need to query the database for each comment - which is not feasible in a distributed system.

The producer uses `aiokafka.AIOKafkaProducer` with JSON serialization (`json.dumps().encode('utf-8')`).

Similarly, when a new post is created, a message is published to `posts_topic`.

#### 6.1.5 Kafka Consumer (Reply Listener)

A background asyncio task (`kafka_consumer_task`) continuously reads from `replies_topic`:

1. Creates an `AIOKafkaConsumer` with group ID `api-gateway-group`
2. Starts consuming in an infinite loop
3. For each message, extracts `comment_id`, `reply_text`, `model`, and `post_id`
4. Inserts the reply into the `ai_replies` SQLite table
5. Broadcasts the reply to all connected WebSocket clients via `manager.broadcast()`

This consumer runs as a background task that starts on application startup (`@app.on_event("startup")`) and runs for the entire lifetime of the API Gateway.

#### 6.1.6 WebSocket Server

The WebSocket endpoint (`/ws/{user_id}`) provides real-time delivery of AI replies:

- A `ConnectionManager` class maintains a dictionary of active WebSocket connections keyed by user_id
- When a user opens the frontend, a WebSocket connection is established
- When the Kafka consumer receives an AI reply, it calls `manager.broadcast()` which sends the reply to ALL connected clients (not just the comment author)
- The broadcast message format: `{"type": "ai_reply", "data": {reply_details}}`

#### 6.1.7 HDFS Integration

The API Gateway communicates with HDFS via the WebHDFS REST API for bulk operations:

- **Upload to HDFS**: `upload_to_hdfs(local_path, hdfs_path)` - Uses the two-step WebHDFS protocol (PUT CREATE redirect, then PUT data to DataNode)
- **Read from HDFS**: `read_from_hdfs(hdfs_path)` - Uses `GET /webhdfs/v1/<path>?op=OPEN` with redirect following
- **Download from HDFS**: For bulk job results, reads the HDFS file and returns it as a `FileResponse`

#### 6.1.8 Image Upload and Captioning

When a user creates a post with an image:

1. The image is received as a base64-encoded string in the request body
2. The API Gateway sends the base64 image to the Inference Service's `/describe-image` endpoint
3. The Inference Service uses LLaMA 4 Scout 17B (a vision-capable model) to generate a caption
4. The caption is stored in the `posts.image_caption` column
5. The image data is stored as a data URL (`data:image/...;base64,...`) in `posts.image_url`

#### 6.1.9 Seed Data

On first startup, if the database is empty, the API Gateway seeds it with:
- 6 demo users (alice, bob, charlie, diana, eve, frank) with password "password123"
- 6 posts across different subreddits (technology, gaming, science, cooking, travel, music) with predefined content and images from Picsum
- 13 comments spread across the first three posts, from various users

Image captions for seed posts are generated by calling the Inference Service's `/describe-image` endpoint with the Picsum image URLs.

#### 6.1.10 Bulk Upload Workflow

1. User uploads a JSON file via `POST /api/bulk/upload` (multipart form data)
2. API Gateway saves the file locally in `/tmp/bulk_uploads/`
3. Uploads the file to HDFS at `/data/raw/bulk/<job_id>/<filename>`
4. Creates a `batch_jobs` record with status "pending"
5. Sends an HTTP POST to `spark-batch-service:8085/process` with job details:
   ```json
   {
     "job_id": "uuid",
     "input_path": "/data/raw/bulk/<job_id>/<filename>",
     "output_path": "/data/replies/bulk/<job_id>"
   }
   ```
6. Returns the job_id to the frontend

#### 6.1.11 Dependencies

```
fastapi          # Web framework
uvicorn          # ASGI server
aiokafka         # Async Kafka client
pyjwt            # JWT token handling
python-multipart # File upload support
httpx            # Async HTTP client
websockets       # WebSocket protocol support
aiofiles         # Async file I/O
pydantic         # Data validation
python-dotenv    # Environment variable loading
```

---

### 6.2 Inference Service (Python/FastAPI)

**File**: `services/inference-service/app/main.py` (1,054 lines)  
**Port**: 8000  
**Framework**: FastAPI with Uvicorn (2 workers)  
**Role**: LLM inference with multi-key pooling, rate limiting, multi-comment batching, and image captioning

The Inference Service is the AI brain of the system. It receives batches of comments from Spark, calls the Groq Cloud API to generate replies using LLaMA models, and returns the generated text. It implements sophisticated rate limiting and batching strategies to maximize throughput while respecting API limits.

#### 6.2.1 Provider Pattern

The service uses a provider pattern to support multiple LLM backends:

```python
class LLMProvider(ABC):
    @abstractmethod
    async def generate(self, prompt: str, system_prompt: str) -> str: ...

    @abstractmethod
    async def generate_with_image(self, prompt: str, image_data: str) -> str: ...
```

Three providers are implemented:
- **MockLLMProvider**: Returns canned responses for testing without API calls
- **GroqLLMProvider**: Connects to Groq Cloud API (the active provider)
- **OpenAILLMProvider**: Connects to OpenAI-compatible APIs (alternative)

The active provider is selected via the `LLM_PROVIDER` environment variable (default: `"groq"`).

#### 6.2.2 GroqKeyPool - Multi-Key API Management

The most sophisticated component of the inference service. The `GroqKeyPool` manages multiple API keys and distributes requests across them:

```python
class GroqKeyPool:
    def __init__(self):
        self.keys = []
        # Load up to 5 API keys from environment variables:
        # GROQ_API_KEY_1, GROQ_API_KEY_2, ..., GROQ_API_KEY_5
        # Falls back to single GROQ_API_KEY if numbered keys not found
        for i in range(1, 6):
            key = os.getenv(f"GROQ_API_KEY_{i}")
            if key:
                self.keys.append(GroqKeySlot(key, i))

    def get_next_key(self) -> GroqKeySlot:
        # Round-robin selection across all available keys
        slot = self.keys[self.current_index]
        self.current_index = (self.current_index + 1) % len(self.keys)
        return slot
```

Each `GroqKeySlot` wraps a single API key with:
- Its own `RateLimiter` instance (independent rate tracking)
- Call counter and error counter for monitoring
- Its own `httpx.AsyncClient` for API calls
- The model name and base URL configuration

**Why multiple keys?**: Groq's free tier limits each API key to approximately 30 requests per minute. By using 5 keys, the system achieves approximately 140 requests per minute (28 RPM per key with safety margin), enabling batch processing of thousands of comments.

#### 6.2.3 RateLimiter - Sliding Window

Each API key has its own `RateLimiter` that implements a sliding-window algorithm:

```python
class RateLimiter:
    def __init__(self, max_requests: int = 28, window_seconds: int = 60):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.timestamps = []

    async def acquire(self):
        while True:
            now = time.time()
            # Remove timestamps older than the window
            self.timestamps = [t for t in self.timestamps if now - t < self.window_seconds]
            if len(self.timestamps) < self.max_requests:
                self.timestamps.append(now)
                return  # Allowed to proceed
            # Calculate how long to wait
            sleep_time = self.timestamps[0] + self.window_seconds - now
            await asyncio.sleep(sleep_time)
```

The sliding window approach (vs. fixed windows) prevents bursts at window boundaries. Each key tracks its own 60-second window of request timestamps.

#### 6.2.4 Multi-Comment Batching

The most important optimization. Instead of making one LLM call per comment, the service packs up to 10 comments into a single LLM call:

**Same-Post Batching** (`generate_multi`):
When multiple comments are about the same post, they are grouped together:
```
System: You are a casual Reddit user...

Post: "What's your favorite programming language?"

Comments:
[1] "I love Python for its simplicity"
[2] "Rust is amazing for performance"
[3] "TypeScript changed my life"

Reply to each comment with [1], [2], [3] format.
```

One LLM call generates all 3 replies. The response is parsed using regex: `\[(\d+)\]\s*(.*?)(?=\[\d+\]|$)`

**Cross-Post Batching** (`generate_mixed_batch`):
When comments come from different posts (singletons), they are packed together with post context:
```
System: You are a casual Reddit user...

=== Item 1 ===
Post: "Best hiking trails"
Comment: "Try the Appalachian Trail"

=== Item 2 ===
Post: "Cooking tips"
Comment: "Always salt your pasta water"

Reply with [1], [2] format.
```

**The `/generate` endpoint orchestration**:
1. Receives a batch of items from Spark
2. Groups items by `post_text` (items with the same post go together)
3. For groups with multiple items: chunks into groups of 10, calls `generate_multi` for each chunk
4. For singleton groups: collects them, chunks into groups of 10, calls `generate_mixed_batch`
5. All LLM calls run concurrently via `asyncio.gather(*tasks)`
6. Returns all replies mapped back to their original comment IDs

#### 6.2.5 Image Captioning

The `/describe-image` endpoint accepts either a base64-encoded image or an image URL and generates a text description:

- Uses **LLaMA 4 Scout 17B** (`meta-llama/llama-4-scout-17b-16e-instruct`) - a vision-capable model
- Constructs a multimodal message with both text prompt and image content
- Prompt: "Describe this image in 2-3 concise sentences. Focus on the main subject, setting, and any notable details."
- Returns a plain text description

#### 6.2.6 Endpoints

| Method | Endpoint | Description |
|---|---|---|
| POST | `/generate` | Generate AI replies for a batch of comments |
| POST | `/describe-image` | Generate image caption using vision model |
| GET | `/health` | Health check with key pool statistics |
| GET | `/` | Service info and status |

#### 6.2.7 Concurrency Control

```python
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT_REQUESTS", "15"))
semaphore = asyncio.Semaphore(max(MAX_CONCURRENT, num_keys * 3))
```

A global semaphore limits the maximum number of concurrent LLM calls. This prevents overwhelming the Groq API with too many simultaneous connections. The semaphore is scaled based on the number of API keys available.

#### 6.2.8 Dependencies

```
fastapi       # Web framework
uvicorn       # ASGI server
httpx         # Async HTTP client for Groq API calls
pydantic      # Data validation and settings
python-dotenv # Environment variable loading
pillow        # Image processing (for base64 encoding)
python-multipart # File upload support
```

---

### 6.3 Spark Streaming Service (Scala)

**Files**: 4 Scala source files  
**Build**: SBT 1.9.7, Scala 2.12, Spark 3.5.0  
**Role**: Real-time stream processing of user comments from Kafka

This service is a Spark Structured Streaming application written in Scala. It continuously reads comments from Kafka, preprocesses them, sends them to the inference service for AI reply generation, and publishes the replies back to Kafka.

#### 6.3.1 StreamingApp.scala (244 lines)

**Main entry point and streaming pipeline orchestrator.**

**Lines 1-20 - Imports and Object Declaration**:
```scala
package com.reddit.streaming
import org.apache.spark.sql.{SparkSession, DataFrame, functions => F}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
// ... more imports
object StreamingApp {
```
Imports Spark SQL APIs for DataFrame operations, streaming triggers, and type definitions.

**Lines 21-55 - SparkSession Creation**:
```scala
val spark = SparkSession.builder()
  .appName("RedditStreamProcessor")
  .master("spark://spark-master:7077")
  .config("spark.driver.memory", "1g")
  .config("spark.executor.memory", "1g")
  .config("spark.cores.max", "2")
  .config("spark.sql.streaming.checkpointLocation", "/checkpoints/streaming")
  .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode:9000")
  .getOrCreate()
```
Creates the Spark session connected to the cluster master. Key configs: 1GB driver memory, 1GB executor memory, max 2 cores. The checkpoint location is critical for Structured Streaming fault tolerance - it stores progress offsets so the stream can resume after failures.

**Lines 56-85 - Kafka Source**:
```scala
val kafkaDF = spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "kafka:9092")
  .option("subscribe", "comments_topic")
  .option("startingOffsets", "latest")
  .option("failOnDataLoss", "false")
  .load()
```
Creates a streaming DataFrame from Kafka. `startingOffsets: latest` means it only processes new messages (not historical). `failOnDataLoss: false` prevents crashes if Kafka data expires during processing.

**Lines 86-115 - Schema Definition and JSON Parsing**:
```scala
val schema = StructType(Seq(
  StructField("commentId", StringType),
  StructField("postId", StringType),
  StructField("userId", StringType),
  StructField("username", StringType),
  StructField("commentText", StringType),
  StructField("postText", StringType),
  StructField("postTitle", StringType),
  StructField("imageUrl", StringType),
  StructField("imageCaption", StringType),
  StructField("parentId", StringType),
  StructField("timestamp", StringType)
))
val parsedDF = kafkaDF
  .selectExpr("CAST(value AS STRING) as json")
  .select(F.from_json(F.col("json"), schema).as("data"))
  .select("data.*")
```
Kafka messages arrive as binary key-value pairs. The value is cast to string, then parsed from JSON into a structured DataFrame using the defined schema. The `select("data.*")` flattens the nested struct into top-level columns.

**Lines 116-160 - foreachBatch Processing**:
```scala
val query = parsedDF.writeStream
  .trigger(Trigger.ProcessingTime("5 seconds"))
  .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
    if (!batchDF.isEmpty) {
      // 1. Preprocess
      val cleaned = PreprocessingEngine.process(batchDF)

      // 2. Write cleaned data to HDFS
      cleaned.write.mode("append")
        .json("hdfs://hadoop-namenode:9000/data/cleaned/streaming/")

      // 3. Generate AI replies via inference service
      val repliesRDD = cleaned.rdd.mapPartitionsWithIndex { (idx, iter) =>
        // ... batch and call inference service
      }

      // 4. Write replies to HDFS
      repliesDS.write.mode("append")
        .json("hdfs://hadoop-namenode:9000/data/replies/streaming/")

      // 5. Publish replies to Kafka
      repliesDS.selectExpr("to_json(struct(*)) AS value")
        .write.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("topic", "replies_topic")
        .save()
    }
  }
  .start()
```

The `foreachBatch` sink is the core pattern. Every 5 seconds (trigger interval), Spark checks for new data from Kafka. If there are new messages:
1. Runs the 6-step preprocessing pipeline
2. Saves cleaned data to HDFS for audit/replay
3. Calls the inference service using `mapPartitionsWithIndex` (partition-level processing)
4. Saves replies to HDFS
5. Publishes replies to Kafka `replies_topic` for the API Gateway to consume

**Lines 161-210 - Inference Call with Broadcast**:
```scala
// Create broadcast variable with metadata for each comment
val metadataMap = cleaned.select("commentId", "postText", "postTitle", ...)
  .collect()
  .map(row => row.getString(0) -> Map(
    "postText" -> row.getString(1),
    "postTitle" -> row.getString(2),
    // ...
  )).toMap
val broadcastMeta = spark.sparkContext.broadcast(metadataMap)
```

A critical optimization: The metadata needed for inference (post text, title, image info) is collected to the driver and broadcast to all executors. This avoids the need for each executor to perform expensive distributed DataFrame lookups during `mapPartitionsWithIndex`. The broadcast variable is efficiently distributed once and cached on each executor.

**Lines 211-244 - Query Lifecycle**:
```scala
query.awaitTermination()
```
The streaming query runs indefinitely until the application is stopped.

#### 6.3.2 PreprocessingEngine.scala (207 lines)

**The 6-step data cleaning pipeline for streaming data.**

Detailed step-by-step breakdown is in [Section 9.1](#91-streaming-preprocessing-6-steps).

#### 6.3.3 InferenceClient.scala (130 lines)

**HTTP client that communicates with the inference service.**

**Key method - `batchInfer`**:
```scala
def batchInfer(items: Seq[InferenceItem]): Seq[InferenceResult] = {
  val url = s"$inferenceUrl/generate"
  val payload = items.map(item => Map(
    "comment_id" -> item.commentId,
    "comment_text" -> item.commentText,
    "post_text" -> item.postText,
    "post_title" -> item.postTitle,
    "image_url" -> item.imageUrl,
    "image_caption" -> item.imageCaption,
    "username" -> item.username
  ))
  // HTTP POST with JSON body, 60-second timeout
  val response = Http(url)
    .postData(toJson(payload))
    .header("Content-Type", "application/json")
    .timeout(connTimeoutMs = 5000, readTimeoutMs = 60000)
    .asString
  // Parse response JSON into Seq[InferenceResult]
}
```

Uses `scalaj-http` for synchronous HTTP calls (since it runs inside Spark's `mapPartitionsWithIndex` on worker nodes where async code is not practical). Each call sends a batch of items and receives a batch of results.

The connection timeout is 5 seconds (fail fast if inference service is down), and the read timeout is 60 seconds (allow time for LLM generation).

#### 6.3.4 Models.scala (96 lines)

**Data model case classes**:

```scala
case class CommentRecord(
  commentId: String, postId: String, userId: String, username: String,
  commentText: String, postText: String, postTitle: String,
  imageUrl: String, imageCaption: String, parentId: String, timestamp: String
)

case class InferenceItem(
  commentId: String, commentText: String, postText: String,
  postTitle: String, imageUrl: String, imageCaption: String, username: String
)

case class InferenceResult(
  commentId: String, replyText: String, model: String
)

case class GeneratedReply(
  commentId: String, postId: String, replyText: String,
  model: String, generatedAt: String
)
```

These case classes define the data shapes used throughout the streaming pipeline. Spark can automatically convert between DataFrames and Datasets of case classes using `spark.implicits._`.

#### 6.3.5 Build Configuration (build.sbt)

```scala
scalaVersion := "2.12.18"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.5.0" % "provided",
  "org.apache.spark" %% "spark-streaming" % "3.5.0" % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.0",
  "org.scalaj" %% "scalaj-http" % "2.4.2",
  "com.google.code.gson" % "gson" % "2.10.1"
)
```

Spark core/sql/streaming are marked as `"provided"` because they are already available in the Spark runtime. The Kafka connector and HTTP/JSON libraries are bundled into the fat JAR.

#### 6.3.6 Dockerfile (Multi-Stage Build)

```dockerfile
# Stage 1: Build with SBT
FROM sbtscala/scala-sbt:eclipse-temurin-jammy-11.0.17_8_1.9.7_2.12.18
WORKDIR /app
COPY build.sbt .
COPY project/ project/
RUN sbt update                    # Download dependencies (cached layer)
COPY src/ src/
RUN sbt assembly                  # Build fat JAR

# Stage 2: Runtime with Spark
FROM apache/spark:3.5.0
COPY --from=0 /app/target/scala-2.12/*.jar /opt/spark/jars/app.jar
COPY --from=0 /app/target/scala-2.12/lib/*.jar /opt/spark/jars/
ENTRYPOINT ["/opt/spark/bin/spark-submit", "--class", "com.reddit.streaming.StreamingApp", ...]
```

The multi-stage build first compiles the Scala code into a fat JAR, then copies it into a clean Spark runtime image. This keeps the final image small by excluding build tools.

---

### 6.4 Spark Batch Service (Scala)

**Files**: 4 Scala source files  
**Port**: 8085 (HTTP server for job triggers and status polling)  
**Build**: SBT 1.9.7, Scala 2.12, Spark 3.5.0  
**Role**: Batch processing of uploaded JSON files with progress tracking

The batch service differs from streaming in several key ways:
- It has an **HTTP server** (built with `com.sun.net.httpserver.HttpServer`) that accepts job trigger requests and serves progress/status queries
- It reads from **HDFS** instead of Kafka
- It tracks **progress** using Spark's `LongAccumulator` and a `ConcurrentHashMap`
- It processes all data in a single job rather than continuous micro-batches

#### 6.4.1 BatchApp.scala (322 lines)

**Lines 1-40 - HTTP Server Setup**:
```scala
val server = HttpServer.create(new InetSocketAddress(8085), 0)
server.createContext("/process", new ProcessHandler())
server.createContext("/status", new StatusHandler())
server.createContext("/health", new HealthHandler())
server.setExecutor(Executors.newFixedThreadPool(4))
server.start()
```

A lightweight HTTP server runs on port 8085 with three endpoints:
- `/process` - Accepts POST requests to trigger new batch jobs
- `/status/{jobId}` - Returns current progress/status of a job
- `/health` - Health check endpoint

**Lines 41-90 - ProcessHandler**:
```scala
class ProcessHandler extends HttpHandler {
  def handle(exchange: HttpExchange): Unit = {
    val body = parseJson(exchange.getRequestBody)
    val jobId = body.get("job_id").getAsString
    val inputPath = body.get("input_path").getAsString
    val outputPath = body.get("output_path").getAsString

    // Update status to "processing"
    jobStatuses.put(jobId, JobStatus("processing", 0, 0))

    // Launch processing in background thread
    val thread = new Thread(() => processBatchJob(jobId, inputPath, outputPath))
    thread.setDaemon(true)
    thread.start()

    // Immediately return 200 OK
    sendResponse(exchange, 200, """{"status": "accepted"}""")
  }
}
```

The process handler accepts the job asynchronously - it starts a background thread for the actual processing and immediately returns HTTP 200. This prevents the API Gateway from timing out while waiting for potentially long-running batch jobs.

**Lines 91-200 - processBatchJob**:
```scala
def processBatchJob(jobId: String, inputPath: String, outputPath: String): Unit = {
  // 1. Read JSON from HDFS
  val rawDF = spark.read.option("multiLine", true).json(s"hdfs://hadoop-namenode:9000$inputPath")

  // 2. Run 8-step preprocessing
  val cleaned = BatchPreprocessingEngine.process(rawDF, spark)

  // 3. Create progress accumulator
  val progressAccumulator = spark.sparkContext.longAccumulator("processedRecords")
  val totalRecords = cleaned.count()

  // 4. Start progress monitoring thread
  val progressThread = new Thread(() => {
    while (!Thread.currentThread().isInterrupted) {
      val current = progressAccumulator.value
      jobStatuses.put(jobId, JobStatus("processing", totalRecords, current))
      Thread.sleep(2000)  // Poll every 2 seconds
    }
  })
  progressThread.setDaemon(true)
  progressThread.start()

  // 5. Repartition for granular progress
  val batchSize = 50
  val numPartitions = Math.max(1, (totalRecords / batchSize).toInt)
  val repartitioned = cleaned.repartition(numPartitions)

  // 6. Process with inference
  val repliesRDD = repartitioned.rdd.mapPartitionsWithIndex { (idx, iter) =>
    val records = iter.toList
    val items = records.map(row => /* create InferenceItem */)
    val results = BatchInferenceClient.batchInfer(items)
    progressAccumulator.add(results.size)  // Update progress
    results.iterator
  }

  // 7. Write results to HDFS
  repliesDS.write.mode("overwrite").json(s"hdfs://hadoop-namenode:9000$outputPath")

  // 8. Update final status
  progressThread.interrupt()
  jobStatuses.put(jobId, JobStatus("completed", totalRecords, totalRecords))
}
```

**Progress Tracking Architecture**:
The `LongAccumulator` is a Spark primitive designed for distributed counters. Each executor adds to it after processing a batch of records. A background thread on the driver polls the accumulator every 2 seconds and updates a `ConcurrentHashMap[String, JobStatus]`. The `/status/{jobId}` endpoint reads from this map to serve progress queries.

**Lines 201-260 - StatusHandler**:
```scala
class StatusHandler extends HttpHandler {
  def handle(exchange: HttpExchange): Unit = {
    val path = exchange.getRequestURI.getPath
    val jobId = path.split("/").last
    val status = jobStatuses.get(jobId)
    if (status != null) {
      val progress = if (status.total > 0) (status.processed * 100.0 / status.total).toInt else 0
      sendResponse(exchange, 200, s"""{"status":"${status.state}","progress":$progress,...}""")
    } else {
      sendResponse(exchange, 404, """{"error":"Job not found"}""")
    }
  }
}
```

#### 6.4.2 BatchPreprocessingEngine.scala (236 lines)

**The 8-step data cleaning pipeline for batch data.** Includes all 6 streaming steps plus JSON explosion and intelligent repartitioning.

Detailed step-by-step breakdown is in [Section 9.2](#92-batch-preprocessing-8-steps).

#### 6.4.3 BatchInferenceClient.scala (123 lines)

Similar to the streaming `InferenceClient`, but with a longer read timeout (120 seconds instead of 60) to accommodate larger batches in batch processing:

```scala
val response = Http(url)
  .postData(toJson(payload))
  .header("Content-Type", "application/json")
  .timeout(connTimeoutMs = 5000, readTimeoutMs = 120000)  // 2-minute timeout
  .asString
```

#### 6.4.4 Models.scala (84 lines)

Same structure as streaming models, with slight variations for batch-specific fields. Includes `BatchRecord` and `EnrichedResult` case classes for the enriched output format.

---

### 6.5 Frontend (React/TypeScript/Nginx)

**Files**: `App.tsx` (1,078 lines), `index.css` (1,368 lines), `main.tsx` (10 lines), `nginx.conf` (47 lines)  
**Port**: 3000  
**Role**: User interface with login, feed, batch upload views, and real-time WebSocket updates

The frontend is a single-page React application with three main views, built with TypeScript and styled with a custom dark theme CSS. It runs behind Nginx which serves static files and proxies API/WebSocket requests.

#### 6.5.1 App.tsx - Application Structure (1,078 lines)

The application is structured as a single file with multiple components:

**TypeScript Interfaces (Lines 1-50)**:
```typescript
interface User { id: string; username: string; token: string; }
interface Post {
  id: string; title: string; body: string; image_url?: string;
  image_caption?: string; user_id: string; username: string;
  subreddit: string; upvotes: number; created_at: string;
  comments?: Comment[];
}
interface Comment {
  id: string; post_id: string; user_id: string; username: string;
  text: string; parent_id?: string; created_at: string;
  ai_replies?: AIReply[];
}
interface AIReply {
  id: string; comment_id: string; reply_text: string;
  model?: string; generated_at: string;
}
interface BatchJob {
  id: string; filename: string; status: string; progress: number;
  total_records: number; processed_records: number; created_at: string;
}
```

**LoginScreen Component (Lines 51-150)**:
- Toggle between Login and Register modes
- Form with username, email (register only), and password fields
- Calls `/api/auth/login` or `/api/auth/register`
- Stores JWT token and user info in component state
- Displays error messages from the API

**FeedView Component (Lines 151-600)** - The main Reddit-like feed:
- **Post List**: Fetches all posts from `/api/posts` on mount, displays as cards
- **Create Post**: Modal with title, body, subreddit selector, optional image upload
- **Post Card**: Shows title, body, image, subreddit badge, upvote button, comment count
- **Comments**: Expandable comment section per post, fetched from `/api/posts/{id}/comments`
- **Create Comment**: Text input at the bottom of each post's comment section
- **AI Replies**: Displayed under each comment with a "robot" icon indicator
- **Upvoting**: Optimistic UI update, then `POST /api/posts/{id}/vote`
- **Image Upload**: File input, converts to base64, sends to `/api/upload-image`, gets caption back

**BatchView Component (Lines 601-850)** - Bulk processing interface:
- **File Upload**: Drag-and-drop or click to select JSON file
- **Upload Progress**: Shows upload state, then switches to processing status
- **Job List**: Table of all batch jobs with status badges
- **Polling**: When a job is "processing", polls `/api/bulk/status/{jobId}` every 3 seconds
- **Progress Bar**: Visual progress indicator (percentage based on processed/total records)
- **Download**: When job is "completed", shows download button linking to `/api/bulk/download/{jobId}`

**WebSocket Integration (Lines 851-950)**:
```typescript
useEffect(() => {
  if (!user) return;
  const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  const ws = new WebSocket(`${wsProtocol}//${window.location.host}/ws/${user.id}`);

  ws.onmessage = (event) => {
    const data = JSON.parse(event.data);
    if (data.type === 'ai_reply') {
      // Update the specific comment's ai_replies array in state
      setPosts(prev => prev.map(post => ({
        ...post,
        comments: post.comments?.map(comment =>
          comment.id === data.data.comment_id
            ? { ...comment, ai_replies: [...(comment.ai_replies || []), data.data] }
            : comment
        )
      })));
      toast.success('New AI reply received!');
    }
  };

  return () => ws.close();
}, [user]);
```

When an AI reply arrives via WebSocket, the component immutably updates the React state to add the reply under the correct comment. A toast notification alerts the user.

**App Component (Lines 951-1078)** - Root component:
- Manages `user` state (null = show login, set = show app)
- Manages `activeView` state ("feed" or "batch")
- Renders navigation bar with Feed/Batch toggle and logout button
- Conditionally renders `LoginScreen`, `FeedView`, or `BatchView`

#### 6.5.2 index.css - Dark Theme (1,368 lines)

A comprehensive dark theme CSS file providing:
- Dark background (`#1a1a2e`) with lighter card surfaces (`#16213e`)
- Custom scrollbar styling
- Reddit-like post card layout with vote buttons
- Comment thread styling with indentation for replies
- AI reply styling with distinct visual treatment (robot icon, different background)
- Batch upload area with drag-and-drop styling
- Progress bar animations
- Responsive design for different screen sizes
- Toast notification styling
- Modal styling for create post/comment dialogs
- Form input styling with focus states

#### 6.5.3 nginx.conf (47 lines)

```nginx
server {
    listen 3000;
    server_name localhost;

    # Serve React SPA
    location / {
        root /usr/share/nginx/html;
        index index.html;
        try_files $uri $uri/ /index.html;    # SPA fallback
    }

    # Proxy API requests to API Gateway
    location /api/ {
        proxy_pass http://api-gateway:8001;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        client_max_body_size 50M;            # Allow large file uploads
    }

    # Proxy WebSocket connections
    location /ws/ {
        proxy_pass http://api-gateway:8001;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;       # WebSocket upgrade
        proxy_set_header Connection "upgrade";        # WebSocket connection
        proxy_set_header Host $host;
        proxy_read_timeout 86400;                     # 24-hour timeout for WS
    }
}
```

**Why Nginx?**: The React app runs in the browser and cannot directly access Docker-internal hostnames like `api-gateway:8001`. Nginx solves this by:
1. Serving the React static files (HTML, JS, CSS)
2. Proxying `/api/*` requests to the API Gateway container
3. Proxying `/ws/*` WebSocket connections with proper upgrade headers

The `try_files $uri $uri/ /index.html` directive ensures client-side routing works - any path that does not match a static file is served the `index.html`, allowing React Router to handle the route.

#### 6.5.4 Dockerfile (Multi-Stage Build)

```dockerfile
# Stage 1: Build React app
FROM node:20-alpine AS build
WORKDIR /app
COPY package*.json ./
RUN npm install
COPY . .
RUN npm run build              # Vite builds to /app/dist/

# Stage 2: Serve with Nginx
FROM nginx:alpine
COPY --from=build /app/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 3000
CMD ["nginx", "-g", "daemon off;"]
```

---

### 6.6 Ngrok Tunneling

**Image**: `ngrok/ngrok:latest`  
**Port**: 4042 (Ngrok inspect API)  
**Profile**: `ngrok` (only starts with `--profile ngrok`)  
**Role**: Exposes the local application to the internet via secure tunnel

Ngrok creates a public HTTPS URL that tunnels to the frontend Nginx container on port 3000. This is useful for:
- Demoing the project to a professor without deploying to a server
- Testing from mobile devices
- Sharing the running instance with teammates

**Configuration** (`infrastructure/ngrok/ngrok.yml`):
```yaml
version: "2"
authtoken: ${NGROK_AUTHTOKEN}    # From environment variable
tunnels:
  reddit-app:
    addr: frontend:3000           # Tunnel to frontend container
    proto: http
```

**Start Scripts**: `start-ngrok.bat` (Windows) and `start-ngrok.sh` (Linux/Mac) handle:
1. Loading the `.env` file to get `NGROK_AUTHTOKEN`
2. Running `docker compose --profile ngrok up -d --build`
3. Polling the Ngrok API (`localhost:4042/api/tunnels`) up to 30 times
4. Extracting and printing the public URL

---

## 7. Data Flow: Streaming Pipeline

This section traces the complete journey of a single user comment from browser to AI reply, step by step.

### Step 1: User Posts a Comment
The user types a comment in the React frontend and clicks "Submit". The frontend sends:
```
POST /api/comments
Authorization: Bearer <jwt_token>
Content-Type: application/json

{
  "post_id": "abc-123",
  "text": "I think Python is the best language for beginners",
  "parent_id": null
}
```

### Step 2: API Gateway Processes the Comment
The API Gateway (`main.py`):
1. Validates the JWT token to identify the user
2. Inserts the comment into the SQLite `comments` table with a new UUID
3. Queries the `posts` table to get the post's body, title, image URL, and caption
4. Constructs an enriched Kafka message with both comment and post context
5. Publishes to `comments_topic` via `AIOKafkaProducer`
6. Returns HTTP 201 with the comment details

### Step 3: Kafka Receives and Stores the Message
Kafka:
1. Receives the message on `comments_topic`
2. Assigns it to a partition (using round-robin or key-based hash)
3. Persists the message to disk with an incrementing offset
4. The message is now available for any consumer to read

### Step 4: Spark Structured Streaming Reads from Kafka
Every 5 seconds (trigger interval), Spark:
1. Checks `comments_topic` for new messages since the last checkpoint offset
2. Reads all new messages into a micro-batch DataFrame
3. Casts the binary Kafka value to a UTF-8 string
4. Parses the JSON string into a structured DataFrame using the predefined schema

### Step 5: Preprocessing Pipeline (6 Steps)
The `PreprocessingEngine.process(batchDF)` method runs 6 transformation steps:
1. **Schema Validation**: Filters out any records with null/empty commentId, postId, commentText, postText, or userId
2. **Text Cleaning**: Removes emojis via regex, replaces URLs with `[URL]`, removes special characters, normalizes whitespace
3. **Null Handling**: Fills null optional fields with defaults (imageUrl → "", username → "anonymous", etc.)
4. **Deduplication**: Removes duplicate commentIds within the batch
5. **Image Metadata Enrichment**: Adds `hasImage` and `hasCaption` boolean columns
6. **Feature Structuring**: Adds `wordCount`, `charCount`, `processedAt`, and `isReply` columns

After preprocessing, the cleaned DataFrame is saved to HDFS at `/data/cleaned/streaming/`.

### Step 6: Broadcast Variable Creation
The driver collects metadata for each comment (postText, postTitle, imageUrl, imageCaption, username) into a Scala `Map[String, Map[String, String]]` keyed by commentId. This map is wrapped in a Spark broadcast variable, which efficiently distributes it to all executors.

### Step 7: mapPartitionsWithIndex - Inference Calls
Each Spark partition processes its records:
1. Collects all records in the partition into a `List`
2. For each record, creates an `InferenceItem` using the broadcast metadata
3. Batches items (default batch size from the iterator)
4. Calls `InferenceClient.batchInfer(items)` - an HTTP POST to the inference service
5. Maps results to `GeneratedReply` objects

### Step 8: Inference Service Processes the Batch
The inference service (`/generate` endpoint):
1. Receives the batch of items
2. Groups items by `post_text` (comments on the same post go together)
3. For multi-comment groups: packs up to 10 comments into one LLM prompt
4. For singleton groups: packs up to 10 singletons into one mixed-batch prompt
5. Calls the Groq API for each chunk (concurrently via `asyncio.gather`)
6. Parses the `[1]`, `[2]`, `[3]` formatted responses
7. Maps replies back to their comment IDs
8. Returns the full batch of results

### Step 9: LLM Generates Replies
The Groq API receives:
```
System: You are a casual Reddit user. Reply naturally and conversationally...

Post: "What programming language should beginners learn?"

Comments to reply to:
[1] "I think Python is the best language for beginners"

Reply to each comment using [1], [2], etc. format.
```

The LLaMA 3.1 8B Instant model generates a response like:
```
[1] Totally agree! Python's syntax is so clean and readable.
The learning curve is way less steep than something like C++.
Plus the community is amazing for beginners.
```

### Step 10: Replies Written to HDFS and Kafka
Back in Spark:
1. The replies RDD is converted to a `Dataset[GeneratedReply]`
2. Written to HDFS at `/data/replies/streaming/` as JSON
3. Converted to Kafka format: `to_json(struct(*)) AS value`
4. Published to `replies_topic` via Spark's Kafka DataFrame writer

### Step 11: API Gateway Consumes the Reply
The API Gateway's background Kafka consumer:
1. Reads the new message from `replies_topic`
2. Parses the JSON to extract commentId, replyText, model, and postId
3. Inserts into the `ai_replies` SQLite table
4. Calls `manager.broadcast()` to send to all WebSocket clients

### Step 12: WebSocket Delivers to Browser
The WebSocket message arrives at the frontend:
```json
{
  "type": "ai_reply",
  "data": {
    "id": "reply-uuid",
    "comment_id": "comment-uuid",
    "post_id": "post-uuid",
    "reply_text": "Totally agree! Python's syntax is so clean...",
    "model": "llama-3.1-8b-instant",
    "generated_at": "2025-01-15T10:30:00"
  }
}
```

### Step 13: UI Updates in Real-Time
The React `onmessage` handler:
1. Parses the WebSocket message
2. Finds the correct post and comment in the state tree
3. Appends the AI reply to the comment's `ai_replies` array
4. React re-renders the comment to show the AI reply
5. A toast notification appears: "New AI reply received!"

**Total latency**: Typically 3-8 seconds from comment submission to AI reply display, depending on the Spark trigger interval (5s) and LLM response time.

---

## 8. Data Flow: Batch Pipeline

### Step 1: User Uploads JSON File
The user navigates to the Batch View in the frontend and uploads a JSON file:
```
POST /api/bulk/upload
Authorization: Bearer <jwt_token>
Content-Type: multipart/form-data

file: demo_bulk_5000.json
```

The JSON file structure (generated by `generate_bulk_data.py`):
```json
[
  {
    "postId": "post-uuid",
    "postTitle": "What's your favorite tech stack?",
    "postText": "I've been experimenting with different stacks...",
    "subreddit": "programming",
    "imageUrl": "https://picsum.photos/...",
    "comments": [
      {
        "commentId": "comment-uuid",
        "userId": "user-uuid",
        "username": "techie42",
        "commentText": "MERN stack is great for beginners"
      },
      {
        "commentId": "comment-uuid-2",
        "userId": "user-uuid-2",
        "username": "devguru",
        "commentText": "I prefer Django + React personally"
      }
    ]
  }
]
```

### Step 2: API Gateway Saves and Uploads to HDFS
1. Saves the file locally to `/tmp/bulk_uploads/<job_id>/<filename>`
2. Uploads to HDFS via WebHDFS two-step protocol:
   - `PUT http://hadoop-namenode:9870/webhdfs/v1/data/raw/bulk/<job_id>/<filename>?op=CREATE&overwrite=true`
   - Follows the redirect to the DataNode URL
   - `PUT <datanode-redirect-url>` with the file content
3. Creates a `batch_jobs` record with status "pending"

### Step 3: Trigger Spark Batch Job
API Gateway sends:
```
POST http://spark-batch-service:8085/process
Content-Type: application/json

{
  "job_id": "batch-uuid",
  "input_path": "/data/raw/bulk/batch-uuid/demo_bulk_5000.json",
  "output_path": "/data/replies/bulk/batch-uuid"
}
```

### Step 4: Spark Batch Job Starts
The `BatchApp.ProcessHandler`:
1. Parses the request
2. Updates `jobStatuses` ConcurrentHashMap to "processing"
3. Spawns a daemon thread running `processBatchJob()`
4. Returns HTTP 200 immediately

### Step 5: Read and Preprocess
In the background thread:
1. Reads the JSON from HDFS with `spark.read.option("multiLine", true).json(hdfsPath)`
2. The JSON is an array of posts with nested comment arrays
3. Runs the 8-step preprocessing pipeline (see Section 9.2)
4. Step 1 (Explode) flattens the nested structure into one row per comment
5. Steps 2-7 are identical to the streaming pipeline
6. Step 8 repartitions based on data size for optimal parallelism

### Step 6: Progress Tracking Setup
1. Creates a `LongAccumulator` named "processedRecords"
2. Counts total records: `val total = cleaned.count()`
3. Starts a progress monitoring daemon thread that polls the accumulator every 2 seconds
4. Repartitions the data into `ceil(total / batchSize)` partitions for granular progress reporting

### Step 7: Distributed Inference
`mapPartitionsWithIndex` on each partition:
1. Collects partition records into a List
2. Creates `InferenceItem` objects
3. Calls `BatchInferenceClient.batchInfer(items)` (HTTP POST, 120s timeout)
4. After receiving results, increments the progress accumulator: `accumulator.add(results.size)`
5. Returns the results iterator

### Step 8: Frontend Polls Progress
While Spark is processing, the frontend polls every 3 seconds:
```
GET /api/bulk/status/batch-uuid
```
The API Gateway proxies this to:
```
GET http://spark-batch-service:8085/status/batch-uuid
```
Response:
```json
{
  "status": "processing",
  "progress": 45,
  "total_records": 5000,
  "processed_records": 2250
}
```
The frontend displays a progress bar updating in real-time.

### Step 9: Results Written to HDFS
After all partitions complete:
1. Replies RDD is cached in memory
2. Replies are joined with original cleaned data to create enriched results
3. Results are written to HDFS at `/data/replies/bulk/<job_id>/` as JSON
4. Progress thread is interrupted
5. Job status updated to "completed"

### Step 10: User Downloads Results
When the frontend sees `status: "completed"`, it shows a Download button.
```
GET /api/bulk/download/batch-uuid
```
The API Gateway:
1. Looks up the `batch_jobs` record for the HDFS output path
2. Lists files in the HDFS directory via WebHDFS
3. Reads the JSON part files
4. Returns them as a `FileResponse` with `Content-Disposition: attachment`

---

## 9. Preprocessing Pipelines

Both pipelines use Apache Spark DataFrame operations to clean, validate, and enrich the data before sending it to the LLM for reply generation. The preprocessing ensures data quality, removes noise, and adds useful metadata.

### 9.1 Streaming Preprocessing (6 Steps)

**File**: `services/spark-streaming-scala/src/main/scala/com/reddit/streaming/PreprocessingEngine.scala` (207 lines)

#### Step 1: Schema Validation (Lines 20-45)

```scala
def validateSchema(df: DataFrame): DataFrame = {
  df.filter(
    F.col("commentId").isNotNull && F.col("commentId") =!= "" &&
    F.col("postId").isNotNull && F.col("postId") =!= "" &&
    F.col("commentText").isNotNull && F.col("commentText") =!= "" &&
    F.col("postText").isNotNull && F.col("postText") =!= "" &&
    F.col("userId").isNotNull && F.col("userId") =!= ""
  )
}
```

**Purpose**: Remove records with missing required fields. If a Kafka message is malformed or has null values for critical fields, it is discarded.

**Spark APIs used**:
- `df.filter()` - Filters rows based on a boolean condition
- `F.col()` - References a column by name
- `.isNotNull` - Checks column is not null
- `=!=` - Not-equal-to operator (Spark Column method)
- `&&` - Logical AND between column conditions

#### Step 2: Text Cleaning (Lines 46-90)

```scala
def cleanText(df: DataFrame): DataFrame = {
  val emojiPattern = "[\\x{1F600}-\\x{1F64F}\\x{1F300}-\\x{1F5FF}\\x{1F680}-\\x{1F6FF}\\x{2600}-\\x{26FF}\\x{2700}-\\x{27BF}]"

  df.withColumn("commentText",
      F.trim(F.regexp_replace(
        F.regexp_replace(
          F.regexp_replace(
            F.regexp_replace(F.col("commentText"), emojiPattern, ""),
            "https?://\\S+", "[URL]"
          ),
          "[^a-zA-Z0-9\\s.,!?'\"\\-()\\[\\]]", ""
        ),
        "\\s+", " "
      ))
    )
    .withColumn("postText",
      F.trim(F.regexp_replace(
        F.regexp_replace(F.col("postText"), emojiPattern, ""),
        "\\s+", " "
      ))
    )
    .filter(F.length(F.col("commentText")) > 0)
}
```

**Purpose**: Clean text data to remove noise that could confuse the LLM or waste tokens.

**Cleaning operations (in order)**:
1. Remove emojis using Unicode range regex (covers emoticons, symbols, dingbats)
2. Replace URLs with the literal text `[URL]` (preserves the fact that a URL existed)
3. Remove special characters (keep only alphanumeric, spaces, and basic punctuation)
4. Normalize whitespace (collapse multiple spaces/tabs/newlines into single space)
5. Trim leading/trailing whitespace
6. Filter out records where cleaning resulted in an empty string

**Spark APIs used**:
- `F.regexp_replace(col, pattern, replacement)` - Regex-based string replacement
- `F.trim(col)` - Remove leading/trailing whitespace
- `F.length(col)` - String length
- `.withColumn("name", expr)` - Add/replace a column with a new expression
- Chained `regexp_replace` calls apply transformations sequentially (innermost first)

#### Step 3: Handle Nulls (Lines 91-115)

```scala
def handleNulls(df: DataFrame): DataFrame = {
  df.withColumn("imageUrl", F.coalesce(F.col("imageUrl"), F.lit("")))
    .withColumn("imageCaption", F.coalesce(F.col("imageCaption"), F.lit("")))
    .withColumn("username", F.coalesce(F.col("username"), F.lit("anonymous")))
    .withColumn("postTitle", F.coalesce(F.col("postTitle"), F.lit("")))
    .withColumn("parentId", F.coalesce(F.col("parentId"), F.lit("")))
    .withColumn("timestamp", F.coalesce(F.col("timestamp"),
      F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss")))
}
```

**Purpose**: Replace null values in optional fields with sensible defaults, preventing NullPointerExceptions downstream.

**Spark APIs used**:
- `F.coalesce(col1, col2)` - Returns the first non-null value. If `imageUrl` is null, returns the literal empty string
- `F.lit(value)` - Creates a literal column value
- `F.date_format(timestamp, pattern)` - Formats a timestamp as a string
- `F.current_timestamp()` - Returns the current timestamp

#### Step 4: Deduplication (Lines 116-130)

```scala
def deduplicate(df: DataFrame): DataFrame = {
  df.dropDuplicates("commentId")
}
```

**Purpose**: Remove duplicate comments within the same micro-batch. If Kafka delivers a message twice (at-least-once semantics), this prevents generating duplicate AI replies.

**Spark APIs used**:
- `df.dropDuplicates("columnName")` - Removes rows where the specified column has duplicate values, keeping the first occurrence

#### Step 5: Enrich Image Metadata (Lines 131-160)

```scala
def enrichImageMetadata(df: DataFrame): DataFrame = {
  df.withColumn("hasImage",
      F.when(F.col("imageUrl").isNotNull && F.col("imageUrl") =!= "", true)
       .otherwise(false)
    )
    .withColumn("hasCaption",
      F.when(F.col("imageCaption").isNotNull && F.col("imageCaption") =!= "", true)
       .otherwise(false)
    )
    .withColumn("imageCaption",
      F.when(F.col("hasImage") && !F.col("hasCaption"), F.lit("[Image without caption]"))
       .otherwise(F.col("imageCaption"))
    )
}
```

**Purpose**: Add boolean flags for image presence and provide a default caption for images that lack one.

**Spark APIs used**:
- `F.when(condition, value).otherwise(fallback)` - Conditional expression (SQL CASE WHEN equivalent)
- Boolean column references with `!` for negation

#### Step 6: Structure Features (Lines 161-207)

```scala
def structureFeatures(df: DataFrame): DataFrame = {
  df.withColumn("wordCount",
      F.size(F.split(F.col("commentText"), "\\s+"))
    )
    .withColumn("charCount",
      F.length(F.col("commentText"))
    )
    .withColumn("processedAt",
      F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss")
    )
    .withColumn("isReply",
      F.when(F.col("parentId").isNotNull && F.col("parentId") =!= "", true)
       .otherwise(false)
    )
}
```

**Purpose**: Add derived features that could be useful for analysis or filtering. Word count and character count provide text length metrics. The `isReply` flag distinguishes top-level comments from nested replies.

**Spark APIs used**:
- `F.split(col, pattern)` - Splits a string into an array by regex pattern
- `F.size(arrayCol)` - Returns the length of an array (counts words after splitting by whitespace)
- `F.length(col)` - Returns string length in characters

---

### 9.2 Batch Preprocessing (8 Steps)

**File**: `services/spark-batch-scala/src/main/scala/com/reddit/batch/BatchPreprocessingEngine.scala` (236 lines)

The batch pipeline includes all 6 streaming steps plus 2 additional steps specific to batch processing.

#### Step 1: Explode Nested JSON (Lines 20-55) - BATCH ONLY

```scala
def explodeComments(df: DataFrame): DataFrame = {
  df.select(
    F.col("postId"),
    F.col("postTitle"),
    F.col("postText"),
    F.col("subreddit"),
    F.col("imageUrl"),
    F.explode_outer(F.col("comments")).as("comment")
  ).select(
    F.col("postId"),
    F.col("postTitle"),
    F.col("postText"),
    F.col("subreddit"),
    F.col("imageUrl"),
    F.col("comment.commentId").as("commentId"),
    F.col("comment.userId").as("userId"),
    F.col("comment.username").as("username"),
    F.col("comment.commentText").as("commentText")
  )
}
```

**Purpose**: The uploaded JSON has a nested structure (posts containing arrays of comments). This step flattens it into one row per comment, carrying the post context (title, body, image) along with each comment.

**Spark APIs used**:
- `F.explode_outer(col)` - Explodes an array column into separate rows. Each element in the `comments` array becomes its own row. `explode_outer` (vs. `explode`) preserves posts that have an empty comments array (they produce a row with null comment fields).
- `.as("alias")` - Renames the exploded column
- `F.col("comment.fieldName")` - Accesses nested struct fields using dot notation

**Example transformation**:
```
Before (1 row):
| postId | postTitle  | comments: [{commentId: "c1", text: "Hello"}, {commentId: "c2", text: "World"}] |

After (2 rows):
| postId | postTitle  | commentId | commentText |
| p1     | My Post    | c1        | Hello       |
| p1     | My Post    | c2        | World       |
```

#### Steps 2-7: Same as Streaming Steps 1-6

These steps are identical in logic to the streaming preprocessing pipeline:
- **Step 2**: Schema Validation (filter null/empty required fields)
- **Step 3**: Clean Text (remove emojis, URLs, special chars, normalize whitespace) - also cleans `postTitle`
- **Step 4**: Handle Nulls (coalesce optional fields with defaults)
- **Step 5**: Deduplication (dropDuplicates on commentId)
- **Step 6**: Enrich Image Metadata (hasImage, hasCaption booleans)
- **Step 7**: Structure Features (wordCount, charCount, processedAt) - does NOT add `isReply` since batch data has no parent_id concept

#### Step 8: Intelligent Repartition (Lines 200-236) - BATCH ONLY

```scala
def intelligentRepartition(df: DataFrame, spark: SparkSession): DataFrame = {
  val count = df.count()
  val targetRecordsPerPartition = 500
  val optimalPartitions = Math.max(1, Math.ceil(count.toDouble / targetRecordsPerPartition).toInt)
  val maxPartitions = spark.sparkContext.defaultParallelism * 2
  val finalPartitions = Math.min(optimalPartitions, maxPartitions)

  df.repartition(finalPartitions)
}
```

**Purpose**: Optimize the number of partitions based on actual data size. Too few partitions means underutilization of the cluster. Too many partitions means excessive scheduling overhead and small task sizes.

**Strategy**:
- Target: 500 records per partition (tuned for the inference service's batch size)
- Upper limit: 2x the cluster's default parallelism (prevents too many tiny partitions)
- Minimum: 1 partition (handles small datasets)

**Spark APIs used**:
- `df.count()` - Triggers computation and returns total row count
- `df.repartition(n)` - Redistributes data across n partitions (full shuffle)
- `spark.sparkContext.defaultParallelism` - Returns the cluster's default parallelism level

---

## 10. LLM Inference Strategy

### 10.1 Multi-Key API Pooling

The system uses up to 5 Groq API keys, configured via environment variables:
```
GROQ_API_KEY_1=gsk_xxxxx...
GROQ_API_KEY_2=gsk_yyyyy...
GROQ_API_KEY_3=gsk_zzzzz...
GROQ_API_KEY_4=gsk_aaaaa...
GROQ_API_KEY_5=gsk_bbbbb...
```

Each key is wrapped in a `GroqKeySlot` with:
- Its own `RateLimiter(max_requests=28, window_seconds=60)`
- Its own `httpx.AsyncClient`
- Call counter and error counter
- Model configuration (default: `llama-3.1-8b-instant`)

The `GroqKeyPool` distributes requests round-robin across all available keys. When `get_next_key()` is called, it returns the next key in circular order. Each key independently tracks its own rate limit window.

**Throughput calculation**:
- 5 keys x 28 RPM per key = **140 requests per minute** total
- With multi-comment batching (10 comments per request), effective throughput = **1,400 comments per minute**

### 10.2 Rate Limiting

The `RateLimiter` implements a **sliding window** algorithm:

```
Timeline: |----60 seconds----|
Requests: [r1 r2 r3 ... r28]

When r29 arrives:
- Check: 28 requests in last 60 seconds (at capacity)
- Calculate: r1 timestamp + 60s - now = sleep_time
- Sleep for sleep_time seconds
- r1 falls out of the window
- r29 is allowed to proceed
```

This is more efficient than fixed-window rate limiting because:
- Fixed windows can allow 2x the rate at window boundaries (28 at end of window + 28 at start of next)
- Sliding windows distribute requests evenly over time

### 10.3 Multi-Comment Batching

**Problem**: Making one LLM call per comment is wasteful. With 5000 comments and 140 RPM, it would take 36 minutes.

**Solution**: Pack up to 10 comments into a single LLM call, reducing API calls by 10x.

**Same-Post Batching** (when multiple comments share the same post):
```
Prompt:
Post: "What's the best programming language?"

Comments:
[1] User alice: "Python is great for beginners"
[2] User bob: "Rust for performance-critical code"
[3] User charlie: "TypeScript for web development"

Reply to each comment. Use [1], [2], [3] format.
```

**Cross-Post Batching** (when comments come from different posts):
```
Prompt:
=== Item 1 ===
Post: "Best hiking trails?"
Comment by hiker99: "Try the PCT"

=== Item 2 ===
Post: "Cooking tips"
Comment by chef42: "Salt your pasta water"

Reply to each. Use [1], [2] format.
```

**Response Parsing**: The regex `\[(\d+)\]\s*(.*?)(?=\[\d+\]|$)` extracts numbered replies. If regex parsing fails, a fallback line-by-line parser is used.

**With batching**: 5000 comments / 10 per call = 500 API calls. At 140 RPM = 3.6 minutes.

### 10.4 Prompt Engineering

**System Prompt**:
```
You are a casual Reddit user engaging in conversations. Reply naturally and 
conversationally. Keep your responses concise but engaging. Match the tone 
and style of the post and comment you're replying to. Be helpful, witty, 
or supportive as appropriate for the context. Do NOT mention that you are 
an AI or language model.
```

**Design decisions**:
- "Casual Reddit user" sets the register (informal, conversational)
- "Match the tone" ensures replies are contextually appropriate
- "Do NOT mention that you are an AI" prevents meta-responses
- The system prompt is the same for both streaming and batch processing

### 10.5 Image Captioning

When a post has an image, the system generates a text caption:

- **Model**: LLaMA 4 Scout 17B (`meta-llama/llama-4-scout-17b-16e-instruct`) - a multimodal model that understands both text and images
- **Input**: Either a base64-encoded image or a URL
- **Prompt**: "Describe this image in 2-3 concise sentences. Focus on the main subject, setting, and any notable details."
- **Output**: Plain text description (e.g., "A golden retriever running through a meadow at sunset. The dog appears happy and energetic.")

The caption is stored in the post record and included in the comment context sent to the LLM, so AI replies can reference the image content.

---

## 11. Script Files (Line-by-Line)

### 11.1 generate_bulk_data.py

**File**: `scripts/generate_bulk_data.py` (481 lines)  
**Purpose**: Generate realistic bulk JSON files for batch processing demos  
**Usage**: `python generate_bulk_data.py --count 5000 --output demo_bulk_5k.json`

#### Lines 1-15: Imports and Setup
```python
import json, random, uuid, argparse, math
from datetime import datetime, timedelta
```
Standard library imports for JSON generation, randomization, UUID creation, CLI argument parsing, and timestamp generation.

#### Lines 16-70: Subreddit and Username Pools
```python
SUBREDDITS = [
    "technology", "programming", "gaming", "science", "cooking",
    "travel", "music", "movies", "fitness", "photography",
    "books", "art", "history", "nature", "diy",
    "datascience", "machinelearning", "webdev", "cybersecurity", "devops",
    "python", "javascript", "rust", "golang", "kotlin",
    "startups", "entrepreneur", "productivity", "education", "philosophy",
    "space", "astronomy", "physics", "biology", "chemistry"
]  # 35 subreddits

USERNAMES = [
    "TechWizard42", "CodeNinja99", "DataDriven", "PixelPusher", ...
]  # 53 usernames
```
Pools of realistic subreddit names and Reddit-style usernames used for random selection when generating data.

#### Lines 71-140: Post Content Templates
```python
POST_TOPICS = [
    "artificial intelligence", "web development", "cloud computing", ...
]  # 42 topics

TITLE_TEMPLATES = [
    "What's your experience with {topic}?",
    "Best practices for {topic} in 2024",
    "Is {topic} worth learning?",
    ...
]  # 15 title templates

BODY_TEMPLATES = [
    "I've been exploring {topic} recently and wanted to share my thoughts...",
    "After working with {topic} for several years, here's what I've learned...",
    ...
]  # 10 body templates
```
Templates with `{topic}` placeholders that are filled at runtime with randomly selected topics, creating varied but realistic post content.

#### Lines 141-200: Comment Templates
```python
COMMENT_TEMPLATES = [
    "Great post! I've been working with this for a while now.",
    "This is really insightful. Thanks for sharing your experience.",
    ...
]  # 31 normal comment templates

DIRTY_COMMENTS = [
    "!!!! BEST DEALS AT example.com/spam !!!!",
    "🔥🔥🔥 CHECK THIS OUT 🔥🔥🔥",
    "<script>alert('xss')</script>",
    "NULL; DROP TABLE users; --",
    ...
]  # 15 dirty/spam comments
```
Normal comments simulate genuine user engagement. Dirty comments (20% probability) simulate spam, XSS attempts, SQL injection, and other malicious content to test the preprocessing pipeline's cleaning capabilities.

#### Lines 201-300: Post Generation Function
```python
def generate_post(post_index):
    topic = random.choice(POST_TOPICS)
    title = random.choice(TITLE_TEMPLATES).format(topic=topic)
    body = random.choice(BODY_TEMPLATES).format(topic=topic)

    # 35% chance of having an image
    image_url = ""
    if random.random() < 0.35:
        width = random.choice([640, 800, 1024])
        height = random.choice([480, 600, 768])
        image_url = f"https://picsum.photos/{width}/{height}?random={post_index}"

    # Generate 1-8 comments per post
    num_comments = random.randint(1, 8)
    comments = []
    for i in range(num_comments):
        # 20% chance of dirty comment
        if random.random() < 0.20:
            text = random.choice(DIRTY_COMMENTS)
        else:
            text = random.choice(COMMENT_TEMPLATES)

        # 10% chance of duplicate commentId (tests deduplication)
        comment_id = str(uuid.uuid4())
        if comments and random.random() < 0.10:
            comment_id = comments[-1]["commentId"]

        # 5% chance of null postText (tests null handling)
        post_text = body if random.random() > 0.05 else None

        comments.append({
            "commentId": comment_id,
            "userId": str(uuid.uuid4()),
            "username": random.choice(USERNAMES),
            "commentText": text
        })

    return {
        "postId": str(uuid.uuid4()),
        "postTitle": title,
        "postText": body,
        "subreddit": random.choice(SUBREDDITS),
        "imageUrl": image_url,
        "comments": comments
    }
```

Each generated post has:
- A random topic-based title and body from templates
- 35% chance of an image (from Picsum random image service)
- 1-8 comments, where 20% are dirty/spam content
- 10% chance of duplicate comment IDs (to test deduplication)
- 5% chance of null postText (to test null handling)

#### Lines 301-400: Main Generation Logic
```python
def generate_bulk_data(target_comments, output_file):
    posts = []
    total_comments = 0

    while total_comments < target_comments:
        post = generate_post(len(posts))
        posts.append(post)
        total_comments += len(post["comments"])

    # Write to file
    with open(output_file, 'w') as f:
        json.dump(posts, f, indent=2)

    return posts, total_comments
```

Generates posts until the target comment count is reached. The output is a JSON array of post objects.

#### Lines 401-481: CLI and Statistics
```python
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate bulk data for batch processing")
    parser.add_argument("--count", type=int, default=100, help="Target number of comments")
    parser.add_argument("--output", type=str, default="bulk_data.json", help="Output file path")
    args = parser.parse_args()

    posts, total = generate_bulk_data(args.count, args.output)

    # Print statistics
    print(f"Generated {len(posts)} posts with {total} total comments")
    print(f"Dirty comments: ~{total * 0.20:.0f} ({20}%)")
    print(f"Posts with images: ~{len(posts) * 0.35:.0f} ({35}%)")

    # Estimate processing time based on Groq rate limits
    api_calls = math.ceil(total / 10)          # 10 comments per LLM call
    minutes = api_calls / 140                   # 140 RPM with 5 keys
    print(f"Estimated LLM API calls: {api_calls}")
    print(f"Estimated processing time: {minutes:.1f} minutes")
```

The CLI allows specifying the target comment count and output file. After generation, it prints statistics and estimates processing time based on the system's known rate limits (140 RPM with 5 keys, 10 comments per call).

---

### 11.2 simulate_velocity.py

**File**: `scripts/simulate_velocity.py` (238 lines)  
**Purpose**: Simulate high-velocity comment traffic for stress testing  
**Usage**: `python simulate_velocity.py --rate 100 --duration 60`

#### Lines 1-20: Imports
```python
import asyncio, httpx, time, random, uuid, argparse, json
```
Uses `asyncio` for async I/O, `httpx` for async HTTP requests, and standard libraries for randomization and CLI.

#### Lines 21-60: Configuration and User Pool
```python
API_BASE = "http://localhost:3000/api"

USERNAMES = ["speedster1", "velocity_test", "load_runner", ...]  # 20 test usernames
POST_TITLES = ["Performance Test Post", "Load Testing Thread", ...]  # 15 post titles
COMMENT_TEXTS = ["Testing under load", "High velocity comment", ...]  # 30 comment texts
```

#### Lines 61-120: User Registration and Post Creation
```python
async def setup_users(client: httpx.AsyncClient) -> list:
    users = []
    for username in USERNAMES[:20]:
        response = await client.post(f"{API_BASE}/auth/register", json={
            "username": username,
            "password": "testpass123",
            "email": f"{username}@test.com"
        })
        if response.status_code == 200:
            users.append(response.json())
    return users

async def create_posts(client: httpx.AsyncClient, users: list) -> list:
    posts = []
    for i in range(15):
        user = random.choice(users)
        response = await client.post(f"{API_BASE}/posts", json={
            "title": random.choice(POST_TITLES),
            "body": f"Load test post #{i}",
            "subreddit": "loadtest"
        }, headers={"Authorization": f"Bearer {user['token']}"})
        if response.status_code in (200, 201):
            posts.append(response.json())
    return posts
```

Creates 20 users and 15 posts as setup before the velocity test begins.

#### Lines 121-200: Velocity Simulation Core
```python
async def simulate_velocity(rate: int, duration: int):
    async with httpx.AsyncClient(timeout=30.0) as client:
        users = await setup_users(client)
        posts = await create_posts(client, users)

        start_time = time.time()
        total_sent = 0
        errors = 0

        while time.time() - start_time < duration:
            batch_start = time.time()
            batch_size = min(rate, 50)  # Send in batches of max 50

            tasks = []
            for _ in range(batch_size):
                user = random.choice(users)
                post = random.choice(posts)
                task = client.post(f"{API_BASE}/comments", json={
                    "post_id": post["id"],
                    "text": random.choice(COMMENT_TEXTS)
                }, headers={"Authorization": f"Bearer {user['token']}"})
                tasks.append(task)

            results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in results:
                if isinstance(result, Exception):
                    errors += 1
                elif result.status_code in (200, 201):
                    total_sent += 1
                else:
                    errors += 1

            # Throttle to target rate
            elapsed = time.time() - batch_start
            expected_time = batch_size / rate
            if elapsed < expected_time:
                await asyncio.sleep(expected_time - elapsed)

        print(f"Sent: {total_sent}, Errors: {errors}, Rate: {total_sent/duration:.1f}/s")
```

**How the velocity simulation works**:
1. Sends comments in batches (max 50 per batch) using `asyncio.gather` for concurrency
2. After each batch, calculates how long it took vs. how long it should have taken at the target rate
3. If the batch completed faster than expected, sleeps for the remaining time (throttling)
4. If the batch took longer than expected, immediately starts the next batch (best-effort rate)
5. Continues for the specified duration, then prints statistics

#### Lines 201-238: CLI Entry Point
```python
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simulate comment velocity")
    parser.add_argument("--rate", type=int, default=100, help="Comments per second")
    parser.add_argument("--duration", type=int, default=30, help="Duration in seconds")
    args = parser.parse_args()

    print(f"Simulating {args.rate} comments/sec for {args.duration} seconds")
    print(f"Expected total: {args.rate * args.duration} comments")
    asyncio.run(simulate_velocity(args.rate, args.duration))
```

---

### 11.3 start-ngrok.bat / start-ngrok.sh

**Files**: `scripts/start-ngrok.bat` (87 lines), `scripts/start-ngrok.sh` (94 lines)  
**Purpose**: Start the Ngrok tunnel and retrieve the public URL  
**Usage**: `start-ngrok.bat` (Windows) or `./start-ngrok.sh` (Linux/Mac)

Both scripts perform the same operations in their respective shell languages:

#### Phase 1: Environment Setup (Lines 1-25)
```bash
# Load .env file
if [ -f "../infrastructure/.env" ]; then
    export $(grep -v '^#' ../infrastructure/.env | xargs)
fi

# Verify NGROK_AUTHTOKEN is set
if [ -z "$NGROK_AUTHTOKEN" ]; then
    echo "ERROR: NGROK_AUTHTOKEN not set in .env file"
    exit 1
fi
```
Loads the `.env` file from the infrastructure directory and checks that the Ngrok auth token is configured.

#### Phase 2: Docker Compose (Lines 26-45)
```bash
docker compose -f ../infrastructure/docker-compose.yml --profile ngrok up -d --build
```
Starts all services including the Ngrok container (which is under the `ngrok` profile and only starts when explicitly requested with `--profile ngrok`).

#### Phase 3: URL Discovery (Lines 46-87)
```bash
MAX_RETRIES=30
RETRY_COUNT=0

while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    RESPONSE=$(curl -s http://localhost:4042/api/tunnels 2>/dev/null)
    if [ $? -eq 0 ] && [ -n "$RESPONSE" ]; then
        PUBLIC_URL=$(echo $RESPONSE | python -c "
            import sys, json
            data = json.load(sys.stdin)
            tunnels = data.get('tunnels', [])
            if tunnels:
                print(tunnels[0].get('public_url', ''))
        " 2>/dev/null)

        if [ -n "$PUBLIC_URL" ]; then
            echo "=========================================="
            echo "  Public URL: $PUBLIC_URL"
            echo "=========================================="
            exit 0
        fi
    fi
    RETRY_COUNT=$((RETRY_COUNT + 1))
    sleep 2
done

echo "ERROR: Could not retrieve Ngrok URL after $MAX_RETRIES attempts"
exit 1
```

Polls the Ngrok local API (`localhost:4042/api/tunnels`) every 2 seconds for up to 30 attempts (60 seconds total). When the tunnel is established, Ngrok's API returns a JSON response containing the public URL (e.g., `https://abc123.ngrok-free.app`). The script extracts this URL using Python's JSON parser and prints it.

---

## 12. Configuration Details

### 12.1 Docker Compose Services

**File**: `infrastructure/docker-compose.yml` (428 lines)

| Service | Image / Build Context | Ports | Dependencies | Health Check |
|---|---|---|---|---|
| `zookeeper` | `confluentinc/cp-zookeeper:7.5.0` | 2181 | None | `echo ruok \| nc localhost 2181` |
| `kafka` | `confluentinc/cp-kafka:7.5.0` | 9092, 29092 | zookeeper (healthy) | `kafka-broker-api-versions --bootstrap-server localhost:9092` |
| `kafka-init` | `confluentinc/cp-kafka:7.5.0` (init) | None | kafka (healthy) | One-shot: creates topics then exits |
| `hadoop-namenode` | `bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8` | 9870, 9000 | None | `curl -f http://localhost:9870` |
| `hadoop-datanode` | `bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8` | 9864 | namenode (healthy) | `curl -f http://localhost:9864` |
| `hdfs-init` | `bde2020/hadoop-namenode` (init) | None | namenode, datanode (healthy) | One-shot: creates HDFS dirs then exits |
| `spark-master` | `apache/spark:3.5.0` | 8080, 7077, 4040 | None | `curl -f http://localhost:8080` |
| `spark-worker` | `apache/spark:3.5.0` | 8081 | spark-master | `curl -f http://localhost:8081` |
| `spark-streaming-service` | Build: `services/spark-streaming-scala` | None | kafka, hdfs-init, spark-worker, inference-service | None |
| `spark-batch-service` | Build: `services/spark-batch-scala` | 8085 | hdfs-init, spark-worker, inference-service | `curl -f http://localhost:8085/health` |
| `inference-service` | Build: `services/inference-service` | 8000 | None | `curl -f http://localhost:8000/health` |
| `api-gateway` | Build: `services/api-gateway` | 8001 | kafka (healthy), namenode (healthy), inference-service | `curl -f http://localhost:8001/health` |
| `frontend` | Build: `services/frontend` | 3000 | api-gateway | None |
| `ngrok` | `ngrok/ngrok:latest` | 4042 | frontend | Profiles: ["ngrok"] |

**Init Containers**: `kafka-init` and `hdfs-init` are configured with `restart: "no"` - they run once at startup to create topics and directories, then exit. They use `depends_on` with `condition: service_healthy` to ensure dependencies are ready.

**Profiles**: The `ngrok` service is under the `ngrok` profile, meaning it only starts when `--profile ngrok` is passed to `docker compose up`.

### 12.2 Kafka Configuration

```yaml
KAFKA_BROKER_ID: 1
KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:29092
KAFKA_NUM_PARTITIONS: 6
KAFKA_DEFAULT_REPLICATION_FACTOR: 1
KAFKA_LOG_RETENTION_HOURS: 168            # 7 days
KAFKA_LOG_RETENTION_BYTES: 1073741824     # 1 GB per partition
KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
```

**Two listeners explained**:
- `PLAINTEXT://kafka:9092` - Used by Docker containers (Spark, API Gateway) to connect to Kafka using the Docker internal network hostname `kafka`
- `PLAINTEXT_HOST://localhost:29092` - Used by tools running on the host machine (for debugging/monitoring) that access Kafka via `localhost`

### 12.3 HDFS Configuration

**NameNode**:
```yaml
CLUSTER_NAME: reddit-cluster
CORE_CONF_fs_defaultFS: hdfs://hadoop-namenode:9000
CORE_CONF_hadoop_http_staticuser_user: root
HDFS_CONF_dfs_webhdfs_enabled: "true"
HDFS_CONF_dfs_permissions_enabled: "false"
HDFS_CONF_dfs_namenode_datanode_registration_ip___hostname___check: "false"
HDFS_CONF_dfs_replication: 1
```

**DataNode**:
```yaml
CORE_CONF_fs_defaultFS: hdfs://hadoop-namenode:9000
HDFS_CONF_dfs_replication: 1
SERVICE_PRECONDITION: hadoop-namenode:9870    # Wait for NameNode
```

**Key settings explained**:
- `dfs_replication: 1` - Only one copy of each block (single node setup; production would use 3)
- `dfs_permissions_enabled: false` - No HDFS permission checks (simplifies development)
- `dfs_webhdfs_enabled: true` - Enables the REST API used by the API Gateway

### 12.4 Spark Configuration

**Spark Master**:
```yaml
SPARK_MODE: master
SPARK_MASTER_WEBUI_PORT: 8080
SPARK_RPC_AUTHENTICATION_ENABLED: "no"
SPARK_RPC_ENCRYPTION_ENABLED: "no"
SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED: "no"
SPARK_SSL_ENABLED: "no"
```

**Spark Worker**:
```yaml
SPARK_MODE: worker
SPARK_MASTER_URL: spark://spark-master:7077
SPARK_WORKER_MEMORY: 6G
SPARK_WORKER_CORES: 4
SPARK_WORKER_WEBUI_PORT: 8081
```

**Application-level config** (in Scala SparkSession):
```
spark.driver.memory = 1g (streaming) / 2g (batch)
spark.executor.memory = 1g
spark.cores.max = 2
spark.hadoop.fs.defaultFS = hdfs://hadoop-namenode:9000
spark.sql.streaming.checkpointLocation = /checkpoints/streaming
```

### 12.5 Environment Variables

**File**: `infrastructure/.env` (49 lines)

| Variable | Default | Description |
|---|---|---|
| `GROQ_API_KEY` | (required) | Single Groq API key (fallback) |
| `GROQ_API_KEY_1` through `GROQ_API_KEY_5` | (optional) | Multiple Groq API keys for pooling |
| `LLM_PROVIDER` | `groq` | LLM provider selection (groq/openai/mock) |
| `LLM_MODEL` | `llama-3.1-8b-instant` | Model name for text generation |
| `LLM_VISION_MODEL` | `meta-llama/llama-4-scout-17b-16e-instruct` | Model for image captioning |
| `GROQ_BASE_URL` | `https://api.groq.com/openai/v1` | Groq API base URL |
| `MAX_CONCURRENT_REQUESTS` | `15` | Max concurrent LLM API calls |
| `COMMENTS_PER_LLM_CALL` | `10` | Max comments packed per LLM call |
| `RATE_LIMIT_RPM` | `28` | Rate limit per key (requests/minute) |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka connection string |
| `INFERENCE_SERVICE_URL` | `http://inference-service:8000` | Inference service URL |
| `HDFS_NAMENODE_URL` | `http://hadoop-namenode:9870` | HDFS WebHDFS URL |
| `JWT_SECRET` | `super-secret-key-change-in-production` | JWT signing secret |
| `NGROK_AUTHTOKEN` | (required for ngrok) | Ngrok authentication token |

---

## 13. Data Transformation at Each Stage

This section shows exactly how data is shaped and transformed as it moves through the system.

### Stage 1: User Input (Frontend)
```json
{
  "post_id": "abc-123",
  "text": "I love Python! 🐍 Check https://python.org for more info!! 😍",
  "parent_id": null
}
```

### Stage 2: Enriched Kafka Message (API Gateway)
```json
{
  "commentId": "comment-uuid-456",
  "postId": "abc-123",
  "userId": "user-uuid-789",
  "username": "pythonista",
  "commentText": "I love Python! 🐍 Check https://python.org for more info!! 😍",
  "postText": "What's your favorite programming language and why?",
  "postTitle": "Programming Language Discussion",
  "imageUrl": "",
  "imageCaption": "",
  "parentId": "",
  "timestamp": "2025-01-15T10:30:00"
}
```
**Transformation**: Post context (postText, postTitle, image info) is joined from the database.

### Stage 3: After Schema Validation (Spark)
No change (record passes validation since all required fields are present).

### Stage 4: After Text Cleaning (Spark)
```json
{
  "commentText": "I love Python Check [URL] for more info",
  "postText": "Whats your favorite programming language and why?"
}
```
**Transformations applied**:
- Emojis 🐍 😍 removed
- URL replaced with `[URL]`
- Special characters `!!` removed
- Multiple spaces normalized

### Stage 5: After Null Handling (Spark)
```json
{
  "imageUrl": "",              // Was already "", no change
  "imageCaption": "",          // Was already "", no change
  "username": "pythonista",    // Was already set, no change
  "parentId": "",              // Was already "", no change
  "timestamp": "2025-01-15T10:30:00"  // Was already set, no change
}
```
No change in this case (all optional fields were already populated).

### Stage 6: After Deduplication (Spark)
No change (commentId is unique in this batch).

### Stage 7: After Image Metadata Enrichment (Spark)
```json
{
  "hasImage": false,           // imageUrl is empty
  "hasCaption": false,         // imageCaption is empty
  "imageCaption": ""           // No change (no image)
}
```

### Stage 8: After Feature Structuring (Spark)
```json
{
  "wordCount": 8,              // "I love Python Check [URL] for more info"
  "charCount": 40,             // Length of cleaned text
  "processedAt": "2025-01-15T10:30:05",
  "isReply": false             // parentId is empty
}
```

### Stage 9: Inference Request (Spark to Inference Service)
```json
[{
  "comment_id": "comment-uuid-456",
  "comment_text": "I love Python Check [URL] for more info",
  "post_text": "Whats your favorite programming language and why?",
  "post_title": "Programming Language Discussion",
  "image_url": "",
  "image_caption": "",
  "username": "pythonista"
}]
```

### Stage 10: LLM Prompt (Inference Service to Groq API)
```
System: You are a casual Reddit user...

Post: "Whats your favorite programming language and why?"

Comment by pythonista: "I love Python Check [URL] for more info"

Reply to this comment naturally and conversationally.
```

### Stage 11: LLM Response (Groq API)
```
[1] Python is definitely a solid choice! The ecosystem is insane -
you've got NumPy, pandas, Django, FastAPI... basically a library
for everything. What kind of projects are you working on with it?
```

### Stage 12: Generated Reply (Spark writes to HDFS and Kafka)
```json
{
  "commentId": "comment-uuid-456",
  "postId": "abc-123",
  "replyText": "Python is definitely a solid choice! The ecosystem is insane...",
  "model": "llama-3.1-8b-instant",
  "generatedAt": "2025-01-15T10:30:08"
}
```

### Stage 13: WebSocket Message (API Gateway to Frontend)
```json
{
  "type": "ai_reply",
  "data": {
    "id": "reply-uuid-012",
    "comment_id": "comment-uuid-456",
    "post_id": "abc-123",
    "reply_text": "Python is definitely a solid choice! The ecosystem is insane...",
    "model": "llama-3.1-8b-instant",
    "generated_at": "2025-01-15T10:30:08"
  }
}
=======
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
>>>>>>> f4a68f33a87a2a14146825f9d5f4a5a904f4ea9e
```

---

<<<<<<< HEAD
## 14. How to Run the Project

### Prerequisites
- Docker and Docker Compose installed
- At least one Groq API key (free at [console.groq.com](https://console.groq.com))
- Minimum 8 GB RAM available for Docker

### Step 1: Configure Environment
Edit `infrastructure/.env` and set your Groq API key(s):
```
GROQ_API_KEY_1=gsk_your_key_here
GROQ_API_KEY_2=gsk_your_second_key    # Optional, for higher throughput
GROQ_API_KEY_3=gsk_your_third_key     # Optional
```

### Step 2: Start All Services
```bash
cd infrastructure
docker compose up -d --build
```

This will:
1. Build 5 custom Docker images (API Gateway, Inference Service, Spark Streaming, Spark Batch, Frontend)
2. Pull 5 pre-built images (Zookeeper, Kafka, HDFS NameNode, HDFS DataNode, Spark)
3. Start all containers in dependency order
4. Run init containers to create Kafka topics and HDFS directories
5. The API Gateway will seed the database with demo users and posts on first startup

### Step 3: Wait for Services
Allow 1-2 minutes for all services to initialize. You can monitor progress:
```bash
docker compose logs -f
```

### Step 4: Access the Application
Open your browser and navigate to `http://localhost:3000`

Login with any of the seeded accounts:
- Username: `alice`, `bob`, `charlie`, `diana`, `eve`, or `frank`
- Password: `password123`

### Step 5: Test Streaming Pipeline
1. Click on any post to expand it
2. Type a comment and submit
3. Wait 3-8 seconds
4. The AI reply will appear under your comment automatically

### Step 6: Test Batch Pipeline
1. Generate test data: `python scripts/generate_bulk_data.py --count 100 --output test.json`
2. Switch to "Batch" view in the UI
3. Upload the `test.json` file
4. Watch the progress bar as the job processes
5. Download the results when complete

### Step 7: (Optional) Public Access via Ngrok
```bash
# Windows
scripts\start-ngrok.bat

# Linux/Mac
./scripts/start-ngrok.sh
```

### Stopping the Project
```bash
cd infrastructure
docker compose down            # Stop containers
docker compose down -v         # Stop and remove volumes (reset all data)
```

---

## 15. UI Ports and Dashboards

| Port | Service | URL | Description |
|---|---|---|---|
| **3000** | Frontend | `http://localhost:3000` | Main application UI (Reddit-like interface) |
| **8080** | Spark Master | `http://localhost:8080` | Spark cluster management UI - shows running apps, workers, memory usage |
| **8081** | Spark Worker | `http://localhost:8081` | Spark worker UI - shows executor details and running tasks |
| **4040** | Spark App | `http://localhost:4040` | Active Spark application UI - shows stages, tasks, DAG visualization |
| **9870** | HDFS NameNode | `http://localhost:9870` | HDFS file browser and cluster health - browse stored files |
| **9864** | HDFS DataNode | `http://localhost:9864` | DataNode status and block reports |
| **8000** | Inference Service | `http://localhost:8000` | Inference service API - health check and key pool stats |
| **8001** | API Gateway | `http://localhost:8001` | API Gateway - direct API access (bypassing Nginx) |
| **8085** | Spark Batch | `http://localhost:8085/health` | Batch service health and job status |
| **4042** | Ngrok | `http://localhost:4042` | Ngrok tunnel inspection UI (only when ngrok profile is active) |

---
=======
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
>>>>>>> f4a68f33a87a2a14146825f9d5f4a5a904f4ea9e
