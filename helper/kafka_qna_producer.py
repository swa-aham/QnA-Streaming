from kafka import KafkaProducer
import psycopg2
import json
import time
from get_question import get_array

# ==== Configuration ====
KAFKA_TOPIC = 'practice7'
KAFKA_SERVER = 'localhost:9092'
START_ID = 1
NUM_QUESTIONS = 300

DB_CONFIG = {
    'dbname': 'exam_db',
    'user': 'exam_db_owner',
    'password': 'npg_mrDsRa8wBvS9',
    'host': 'ep-muddy-firefly-a1keftog-pooler.ap-southeast-1.aws.neon.tech',
    'port': 5432,
    'sslmode': 'require'
}

questions = get_array()  # Get the list of questions

# ==== Kafka Producer ====
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: str(k).encode('utf-8')
)

# ==== PostgreSQL Insert ====
def insert_to_db(conn, qid, question):
    with conn.cursor() as cursor:
        cursor.execute("INSERT INTO exam (id, question) VALUES (%s, %s)", (qid, question))
    conn.commit()

# ==== Main Logic ====
def main():
    print("🚀 Starting QnA Kafka Producer...")

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("✅ Connected to PostgreSQL")

        id = 1
        for question in questions:
            qid = id
            if qid > 30:
                break
            id += 1
            qna_obj = {
                "id": qid,
                "question": question,
                "answer": None  # Explicitly setting null
            }

            # Send to Kafka
            producer.send(KAFKA_TOPIC, key=qid, value=qna_obj)
            print(f"📤 Sent to Kafka -> ID: {qid}, Question: {question}")

            # Insert to DB
            insert_to_db(conn, qid, question)
            print(f"💾 Inserted into DB -> ID: {qid}, Question: {question}")

            time.sleep(0.5)  # Optional delay

        print("✅ All questions sent and saved.")

    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        producer.flush()
        producer.close()
        if conn:
            conn.close()
            print("🔌 PostgreSQL connection closed.")

if __name__ == "__main__":
    main()
