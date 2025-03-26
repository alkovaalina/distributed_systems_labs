import hazelcast
import time

# Підключення до кластера
client = hazelcast.HazelcastClient(cluster_name="my-cluster", cluster_members=["localhost:5701", "localhost:5702", "localhost:5703"])

# Створення Distributed Map
distributed_map = client.get_map("my-distributed-map").blocking()

# Запис 1000 значень
for i in range(1000):
    distributed_map.put(str(i), f"value_{i}")

print("Записано 1000 значень у Distributed Map")
print(f"Розмір мапи: {distributed_map.size()}")

client.shutdown()
