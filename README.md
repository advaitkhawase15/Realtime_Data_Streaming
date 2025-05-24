# 🚀 Real-Time Data Streaming Pipeline

This project demonstrates a complete real-time data streaming pipeline, covering **data ingestion**, **stream processing**, and **persistent storage** using a modern data engineering stack:

- **Apache Kafka** for data ingestion
- **Apache Spark Structured Streaming** for real-time processing
- **Apache Cassandra** for storage
- **Apache Airflow** for orchestration
- **Docker Compose** for containerization

---

## 🛠 Tech Stack

| Layer           | Tool                       |
|----------------|----------------------------|
| Ingestion       | Apache Kafka               |
| Coordination    | Apache Zookeeper           |
| Processing      | Apache Spark (Structured Streaming) |
| Storage         | Apache Cassandra           |
| Orchestration   | Apache Airflow             |
| Metadata Store  | PostgreSQL (Airflow DB)    |
| Containerization| Docker, Docker Compose     |

---

## 🗺️ Architecture Overview
![Screenshot (113)](https://github.com/user-attachments/assets/d59ca5f3-3826-4fc6-b1d3-fb1f2830e7b8)

### Flow:

1. **Data Source**: A simulated API or producer generates user events.
2. **Airflow**: Orchestrates the pipeline and interacts with PostgreSQL for metadata.
3. **Kafka**: Serves as the core message broker for real-time streaming.
4. **Zookeeper**: Manages Kafka brokers.
5. **Spark Structured Streaming**: Reads data from Kafka, processes it, and writes to Cassandra.
6. **Cassandra**: Stores the processed stream data.
7. **Kafka Control Center** & **Schema Registry**: For topic and schema management.

## ⚙️ Setup Instructions

### 1. Clone the Repository

```bash
git clone https://github.com/advaitkhawase15/Realtime_Data_Streaming.git
cd streaming-pipeline
```
### 2. Start All Services with Docker Compose

```bash
docker compose up -d 
```
This will start:
1. Kafka + Zookeeper
2. Spark (Master + Workers)
3. Cassandra
4. Airflow (Webserver + Scheduler)
5. Kafka UI

Access Airflow UI at: http://localhost:8080
Login:
- Username: admin
- Password: admin

and run the DAG..

### 3. Trigger the Spark stream file
Trigger the stream file once the Kafka topic starts having data:
```bash
python spark_stream.py
```
### 4. Verify Data in Cassandra
```bash
docker exec -it cassandra cqlsh -u cassandra -p cassandra localhost 9042
```

Inside the CLI use:
```bash
USE spark_streams;
SELECT * FROM spark_streams.created_users;
```
to see the data in the table.

### 5. To stop all services

```bash
docker compose down --volumes
```




