import hazelcast
import threading
import time

client = hazelcast.HazelcastClient(
    cluster_name="my-cluster",
    cluster_members=["localhost:5701", "localhost:5702", "localhost:5703"]
)

queue = client.get_queue("my-bounded-queue").blocking()

producer_finished = False
stop_put_thread = False

# Функція продюсера
def producer():
    global producer_finished
    print("Producer started writing values from 1 to 100")
    for i in range(1, 101):
        queue.put(i)
        print(f"Producer wrote: {i}, Queue size: {queue.size()}")
        time.sleep(0.05)  # Затримка для імітації реального сценарію
    print("Producer finished writing")
    producer_finished = True

# Функція консюмера
def consumer(consumer_id):
    print(f"Consumer {consumer_id} started reading")
    while not producer_finished or queue.size() > 0:
        try:
            item = queue.poll(timeout=1.0)
            if item is None:
                # Якщо черга порожня і продюсер закінчив, виходимо
                if producer_finished and queue.size() == 0:
                    break
                continue
            print(f"Consumer {consumer_id} read: {item}, Queue size: {queue.size()}")
        except Exception as e:
            print(f"Consumer {consumer_id} error: {e}")
            break
    print(f"Consumer {consumer_id} finished reading")

# Функція для спроби запису в заповнену чергу
def try_put(queue):
    global stop_put_thread
    try:
        queue.put(11)  # Це має заблокуватися, бо черга заповнена
        print("Unexpected success: 11th item added")
    except Exception as e:
        if not stop_put_thread:
            print(f"Error in try_put: {e}")

# Тестування поведінки при заповненій черзі без читання
def test_full_queue():
    global stop_put_thread
    print("\nTesting behavior when queue is full without consumers")
    queue.clear()
    print("Queue cleared")
    # Записуємо 10 елементів (ліміт черги)
    for i in range(1, 11):
        queue.put(i)
        print(f"Producer wrote to full test: {i}, Queue size: {queue.size()}")
    # Спроба додати 11-й елемент у заповнену чергу
    print("Attempting to write 11th item (queue is full, should block)...")
    start_time = time.time()

    put_thread = threading.Thread(target=try_put, args=(queue,))
    put_thread.daemon = True
    put_thread.start()

    # Чекаємо 5 секунд, щоб перевірити, що put() блокується
    time.sleep(5)
    elapsed = time.time() - start_time
    if put_thread.is_alive():
        print(f"Blocked for {elapsed:.2f} seconds as expected (operation is still blocked)")
    else:
        print("Unexpected: put() did not block")

    stop_put_thread = True

if __name__ == "__main__":

    print("Starting Producer and Consumers demo")
    producer_thread = threading.Thread(target=producer, name="Producer")
    consumer1_thread = threading.Thread(target=consumer, args=(1,), name="Consumer-1")
    consumer2_thread = threading.Thread(target=consumer, args=(2,), name="Consumer-2")
    # Старт потоків
    producer_thread.start()
    consumer1_thread.start()
    consumer2_thread.start()

    producer_thread.join()
    consumer1_thread.join()
    consumer2_thread.join()

    # Тестування заповненої черги
    test_full_queue()

    client.shutdown()
    print("\nDemo completed")
