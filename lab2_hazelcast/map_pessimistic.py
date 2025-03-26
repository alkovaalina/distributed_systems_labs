import hazelcast
import threading
import time

def pessimistic_increment(map_instance, count):
    for _ in range(count):
        map_instance.lock("key")
        try:
            current = map_instance.get("key") or 0
            map_instance.put("key", current + 1)
        finally:
            map_instance.unlock("key")

client = hazelcast.HazelcastClient(
    cluster_name="my-cluster",
    cluster_members=["localhost:5701", "localhost:5702", "localhost:5703"]
)
distributed_map = client.get_map("counter_map").blocking()

distributed_map.put("key", 0)

count = 10_000
thread_num = 3
expected_value = count * thread_num

print("\n---------------->>>")
start_time = time.time()

# Запуск потоків
threads = []
for i in range(thread_num):
    thread = threading.Thread(target=pessimistic_increment, args=(distributed_map, count), name=f"IncThread-{i+1}")
    thread.start()
    threads.append(thread)

for thread in threads:
    thread.join()

# Результати
elapsed = time.time() - start_time
final_value = distributed_map.get("key")
print(f"Final result: {final_value}, time: {elapsed:.2f} seconds")
print(f"Expected result: {expected_value}")

client.shutdown()
