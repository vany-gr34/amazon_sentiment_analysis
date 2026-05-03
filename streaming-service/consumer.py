import os
import json
import logging
from datetime import datetime
from confluent_kafka import Consumer, KafkaError, KafkaException
from pymongo import MongoClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "raw-events")
KAFKA_GROUP = os.environ.get("KAFKA_GROUP_ID", "streaming-consumer")
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongo:27017/mlplatform")

def main():
    conf = {
        'bootstrap.servers': KAFKA_SERVERS,
        'group.id': KAFKA_GROUP,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
    }
    
    consumer = Consumer(conf)
    consumer.subscribe([KAFKA_TOPIC])
    
    mongo = MongoClient(MONGO_URI)
    db = mongo.get_default_database()
    collection = db.processed_events
    
    logger.info(f"Consuming from '{KAFKA_TOPIC}' on {KAFKA_SERVERS}")
    
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(f"Kafka error: {msg.error()}")
                    break
            
            try:
                event = json.loads(msg.value().decode('utf-8'))
                event['processed_at'] = datetime.utcnow().isoformat()
                collection.insert_one(event)
                logger.info(f"Stored: {event}")
            except json.JSONDecodeError as e:
                logger.error(f"JSON error: {e}, raw: {msg.value()[:100]}")
            except Exception as e:
                logger.error(f"Processing error: {e}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        mongo.close()

if __name__ == "__main__":
    main()