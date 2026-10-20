import json
from datetime import datetime

from kafka import KafkaConsumer, KafkaProducer
from configs import kafka_config, MY_NAME

# Назви топіків
input_topic = f"{MY_NAME}_building_sensors"
temperature_topic = f"{MY_NAME}_temperature_alerts"
humidity_topic = f"{MY_NAME}_humidity_alerts"

# Порогові значення
TEMP_MAX = 40
HUMIDITY_MIN = 20
HUMIDITY_MAX = 80

# 1. Consumer читає дані датчиків
consumer = KafkaConsumer(
    input_topic,
    bootstrap_servers=kafka_config["bootstrap_servers"],
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    key_deserializer=lambda k: k.decode("utf-8") if k else None,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id=f"{MY_NAME}_processor_group",
)

# 2. Producer - відправляє сповіщення
producer = KafkaProducer(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: str(k).encode("utf-8"),
)

print(f"Обробник запущено. Читаю {input_topic}. Ctrl+C — зупинити.\n")

try:
    # 3. Безперервно читаємо нові повідомлення
    for message in consumer:
        data = message.value
        sensor_id = data["sensor_id"]
        temperature = data["temperature"]
        humidity = data["humidity"]

        print(f"Отримано: датчик {sensor_id} | t={temperature}°C | h={humidity}%")

        # 4. Перевірка температури
        if temperature > TEMP_MAX:
            alert = {
                "sensor_id": sensor_id,
                "timestamp": data["timestamp"],
                "temperature": temperature,
                "humidity": humidity,
                "alert_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "message": f"Температура {temperature}°C перевищує поріг {TEMP_MAX}°C",
            }
            producer.send(temperature_topic, key=sensor_id, value=alert)
            print(f"Температура -> {temperature_topic}: {alert['message']}")

        # 5. Перевірка вологості
        if humidity > HUMIDITY_MAX or humidity < HUMIDITY_MIN:
            alert = {
                "sensor_id": sensor_id,
                "timestamp": data["timestamp"],
                "temperature": temperature,
                "humidity": humidity,
                "alert_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "message": f"Вологість {humidity}% поза межами {HUMIDITY_MIN}–{HUMIDITY_MAX}%",
            }
            producer.send(humidity_topic, key=sensor_id, value=alert)
            print(f"Вологість -> {humidity_topic}: {alert['message']}")

        producer.flush()

except KeyboardInterrupt:
    print("\nОбробник зупинено.")
finally:
    consumer.close()
    producer.close()