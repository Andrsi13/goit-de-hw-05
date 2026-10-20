import json

from kafka import KafkaConsumer
from configs import kafka_config, MY_NAME

temperature_topic = f"{MY_NAME}_temperature_alerts"
humidity_topic = f"{MY_NAME}_humidity_alerts"

# Consumer підписаний одразу на ДВА топіки
consumer = KafkaConsumer(
    temperature_topic,
    humidity_topic,
    bootstrap_servers=kafka_config["bootstrap_servers"],
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    key_deserializer=lambda k: k.decode("utf-8") if k else None,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id=f"{MY_NAME}_alerts_reader_group",
)

print(f"Читаю сповіщення з {temperature_topic} та {humidity_topic}. Ctrl+C — зупинити.\n")

try:
    for message in consumer:
        alert = message.value

        # З якого топіка прийшло сповіщення
        if message.topic == temperature_topic:
            label = "ТЕМПЕРАТУРА"
        else:
            label = "ВОЛОГІСТЬ"

        print(
            f"[{label}] датчик {alert['sensor_id']} | "
            f"час: {alert['timestamp']} | "
            f"t={alert['temperature']}°C, h={alert['humidity']}% | "
            f"{alert['message']}"
        )

except KeyboardInterrupt:
    print("\nЧитач зупинено.")
finally:
    consumer.close()