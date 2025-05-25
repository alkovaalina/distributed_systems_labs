#!/bin/bash
echo "Чекаємо, поки Consul буде готовий..."
sleep 10
curl -X PUT -d '["kafka1:9092", "kafka2:9093", "kafka3:9094"]' http://consul:8500/v1/kv/config/kafka/brokers
curl -X PUT -d '"messages"' http://consul:8500/v1/kv/config/kafka/topic
curl -X PUT -d '["hazelcast1:5701", "hazelcast2:5701", "hazelcast3:5701"]' http://consul:8500/v1/kv/config/hazelcast/cluster_members
curl -X PUT -d '"dev"' http://consul:8500/v1/kv/config/hazelcast/cluster_name
echo "Key/value сховище Consul ініціалізовано."
