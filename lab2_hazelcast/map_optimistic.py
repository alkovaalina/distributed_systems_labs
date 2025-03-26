import hazelcast
import threading
import time

def optimistic_increment(map_instance, count):
    for _ in range(count):
        while True:
            old_value = map_instance.get("key") or 0
            new_value = old_value + 1
            if map_instance.replace_if_same("key", old_value, new_value):
                break

client = hazelcast.HazelcastClient(
    cluster_name="my-cluster",
    cluster_members=["localhost:5701", "localhost:5702", "localhost:5703"]
)
distributed_map = client.get_map("new_map").blocking()

distributed_map.put("key", 0)

# Налаштування тесту
count = 10_000
thread_num = 3
expected_value = count * thread_num

print("\n---------------->>>")
start_time = time.time()

# Запуск потоків
threads = []
for i in range(thread_num):
    thread = threading.Thread(target=optimistic_increment, args=(distributed_map, count), name=f"IncThread-{i+1}")
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
