from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from configs import kafka_config, MY_NAME

# 1. Підключаємось до Kafka
admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config["bootstrap_servers"],
)

# 2. Створюємо назви топіків
topic_names = [
    f"{MY_NAME}_building_sensors",
    f"{MY_NAME}_temperature_alerts",
    f"{MY_NAME}_humidity_alerts",
]

# 3. Описуємо топіки
new_topics = [
    NewTopic(name=name, num_partitions=2, replication_factor=1)
    for name in topic_names
]

# 4. Створюємо
try:
    admin_client.create_topics(new_topics=new_topics, validate_only=False)
    print("Топіки створено успішно")
except TopicAlreadyExistsError:
    print("Топіки вже існують")

# 5. Виводимо топіки
[print(topic) for topic in admin_client.list_topics() if MY_NAME in topic]

admin_client.close()