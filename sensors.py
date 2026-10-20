import json
import random
import time
from datetime import datetime

from kafka import KafkaProducer
from configs import kafka_config, MY_NAME

# 1. ID датчика
sensor_id = random.randint(1000, 9999)

topic_name = f"{MY_NAME}_building_sensors"

# 2. Створюємо producer
producer = KafkaProducer(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: str(k).encode("utf-8"),
)

print(f"Датчик {sensor_id} запущено. Відправка в топік {topic_name}. Ctrl+C — зупинити.")

try:
    while True:
        # 3. Генеруємо рандомні показники
        data = {
            "sensor_id": sensor_id,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "temperature": round(random.uniform(25, 45), 2),
            "humidity": round(random.uniform(15, 85), 2),
        }

        # 4. Відправляємо в Kafka
        producer.send(topic_name, key=sensor_id, value=data)
        producer.flush()

        print(f"Відправлено: {data}")

        # 5. Затримка між виводами
        time.sleep(2)

except KeyboardInterrupt:
    print(f"\nДатчик {sensor_id} зупинено.")
finally:
    producer.close()