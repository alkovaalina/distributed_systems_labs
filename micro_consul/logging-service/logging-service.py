import atexit
import time
from flask import Flask, request, jsonify, json
import hazelcast
import os
import logging
import consul

tracker = Flask(__name__)
INSTANCE_ID = os.getenv("INSTANCE_ID", "default")
PORT = int(os.getenv("PORT", "8881"))
CONSUL_HOST = os.getenv("CONSUL_HOST", "consul")
CONSUL_PORT = int(os.getenv("CONSUL_PORT", "8500"))

tracker.logger.setLevel(logging.DEBUG)


consul_client = consul.Consul(host=CONSUL_HOST, port=CONSUL_PORT)


def register_service():
    consul_client.agent.service.register(
        service_id=INSTANCE_ID,
        name="logging-service",
        address="logging-service" + INSTANCE_ID[-1],
        port=PORT,
        check=consul.Check.http(f"http://logging-service{INSTANCE_ID[-1]}:{PORT}/health", interval="10s", timeout="5s")
    )
    print(f"[{INSTANCE_ID}] Зареєстровано в Consul як {INSTANCE_ID}")


def deregister_service():
    consul_client.agent.service.deregister(INSTANCE_ID)
    print(f"[{INSTANCE_ID}] Видалено з Consul")


@tracker.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "healthy"}), 200

def get_hazelcast_config():
    for attempt in range(10):
        try:
            _, members_data = consul_client.kv.get("config/hazelcast/cluster_members")
            print(f"[{INSTANCE_ID}] Attempt {attempt + 1}: members_data = {members_data}")
            if members_data is None:
                raise ValueError("Ключ config/hazelcast/cluster_members відсутній у Consul")
            members = json.loads(members_data["Value"].decode("utf-8"))
            _, cluster_name_data = consul_client.kv.get("config/hazelcast/cluster_name")
            print(f"[{INSTANCE_ID}] Attempt {attempt + 1}: cluster_name_data = {cluster_name_data}")
            if cluster_name_data is None:
                raise ValueError("Ключ config/hazelcast/cluster_name відсутній у Consul")
            cluster_name = json.loads(cluster_name_data["Value"].decode("utf-8"))
            print(f"[{INSTANCE_ID}] Successfully retrieved Hazelcast config: members={members}, cluster_name={cluster_name}")
            return members, cluster_name
        except Exception as e:
            print(f"[{INSTANCE_ID}] Attempt {attempt + 1} failed: {e}")
            time.sleep(5)
    raise Exception(f"[{INSTANCE_ID}] Не вдалося отримати конфігурацію Kafka після 10 спроб")



HAZELCAST_MEMBERS, HAZELCAST_CLUSTER_NAME = get_hazelcast_config()
hz = hazelcast.HazelcastClient(
    cluster_members=HAZELCAST_MEMBERS,
    cluster_name=HAZELCAST_CLUSTER_NAME
)
records = hz.get_map("logs").blocking()

@tracker.route("/tracker", methods=["POST", "GET"])
def handle():
    if request.method == "POST":
        if request.is_json:
            payload = request.get_json()
            identifier = payload.get("msg")
            info = payload.get("msg")
        else:
            identifier = request.form.get("key")
            info = request.form.get("data")

        if not (identifier and info):
            return jsonify({"error": "Відсутній ключ або дані"}), 400

        if records.contains_key(identifier):
            tracker.logger.info(f"[{INSTANCE_ID}] Пропущено: {identifier}")
            return jsonify({"status": "Дублікат"}), 201

        records.put(identifier, info)
        tracker.logger.info(f"[{INSTANCE_ID}] Збережено: {info} [Key: {identifier}]")
        return jsonify({"status": "OK"}), 201

    elif request.method == "GET":
        values = records.values()
        return jsonify({"messages": list(values)})

if __name__ == "__main__":
    time.sleep(20)
    register_service()
    atexit.register(deregister_service)
    tracker.logger.info(f"[{INSTANCE_ID}] Запуск logging-service на порті {PORT}")
    tracker.run(host='0.0.0.0', port=PORT)
