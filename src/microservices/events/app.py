import os
import json
import threading
import time
from datetime import datetime
from typing import Dict, Any

from fastapi import FastAPI, HTTPException, status
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError

app = FastAPI(title="CinemaAbyss Events Service")

# Kafka configuration
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092").split(",")

def get_kafka_producer():
    """Создает Kafka producer с повторными попытками подключения"""
    for attempt in range(5):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKERS,
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                max_block_ms=5000
            )
            print(f"✅ Kafka producer connected to {KAFKA_BROKERS}")
            return producer
        except Exception as e:
            print(f"❌ Kafka producer connection attempt {attempt + 1} failed: {e}")
            if attempt < 4:
                time.sleep(2)
            else:
                print("❌ Failed to connect Kafka producer after 5 attempts")
                return None

def kafka_consumer_worker():
    """Фоновый worker для чтения и обработки событий из Kafka"""
    print("🔄 Starting Kafka consumer worker...")
    
    for attempt in range(10):  # Больше попыток для consumer
        try:
            consumer = KafkaConsumer(
                'user-events', 'payment-events', 'movie-events',
                bootstrap_servers=KAFKA_BROKERS,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                group_id='events-service-group',
                auto_offset_reset='earliest'
            )
            print(f"✅ Kafka consumer connected to {KAFKA_BROKERS}")
            break
        except Exception as e:
            print(f"❌ Kafka consumer connection attempt {attempt + 1} failed: {e}")
            if attempt < 9:
                time.sleep(3)
            else:
                print("❌ Failed to connect Kafka consumer after 10 attempts")
                return

    # Обрабатываем сообщения
    try:
        for message in consumer:
            event_data = message.value
            topic = message.topic
            timestamp = datetime.now().isoformat()
            
            print(f"📨 [{timestamp}] Processed event from topic '{topic}': {event_data}")
            
    except Exception as e:
        print(f"❌ Error in Kafka consumer: {e}")

# Запускаем consumer в отдельном потоке
consumer_thread = threading.Thread(target=kafka_consumer_worker, daemon=True)
consumer_thread.start()

@app.get("/healthz")
def healthz():
    return {
        "status": "ok",
        "service": "events",
        "kafka_brokers": KAFKA_BROKERS,
        "timestamp": datetime.now().isoformat()
    }

@app.get("/api/events/health")
def health():
    return {
        "status": True,
        "service": "events"
    }

@app.post("/api/events/user", status_code=status.HTTP_201_CREATED)
async def create_user_event(event: Dict[str, Any]):
    """Создает событие пользователя"""
    try:
        producer = get_kafka_producer()
        if not producer:
            raise HTTPException(status_code=503, detail="Kafka producer not available")
        
        event_data = {
            "event_type": "user",
            "timestamp": datetime.now().isoformat(),
            "data": event
        }
        
        future = producer.send('user-events', value=event_data)
        producer.flush()  # Ждем отправки
        
        print(f"📤 Sent user event: {event_data}")
        return {"status": "success", "topic": "user-events", "event": event_data}
        
    except Exception as e:
        print(f"❌ Error sending user event: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to send event: {str(e)}")

@app.post("/api/events/payment", status_code=status.HTTP_201_CREATED)
async def create_payment_event(event: Dict[str, Any]):
    """Создает событие платежа"""
    try:
        producer = get_kafka_producer()
        if not producer:
            raise HTTPException(status_code=503, detail="Kafka producer not available")
        
        event_data = {
            "event_type": "payment",
            "timestamp": datetime.now().isoformat(),
            "data": event
        }
        
        future = producer.send('payment-events', value=event_data)
        producer.flush()
        
        print(f"📤 Sent payment event: {event_data}")
        return {"status": "success", "topic": "payment-events", "event": event_data}
        
    except Exception as e:
        print(f"❌ Error sending payment event: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to send event: {str(e)}")

@app.post("/api/events/movie", status_code=status.HTTP_201_CREATED)
async def create_movie_event(event: Dict[str, Any]):
    """Создает событие фильма"""
    try:
        producer = get_kafka_producer()
        if not producer:
            raise HTTPException(status_code=503, detail="Kafka producer not available")
        
        event_data = {
            "event_type": "movie",
            "timestamp": datetime.now().isoformat(),
            "data": event
        }
        
        future = producer.send('movie-events', value=event_data)
        producer.flush()
        
        print(f"📤 Sent movie event: {event_data}")
        return {"status": "success", "topic": "movie-events", "event": event_data}
        
    except Exception as e:
        print(f"❌ Error sending movie event: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to send event: {str(e)}") 