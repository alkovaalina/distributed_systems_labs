import requests

def send_messages():
    facade_url = "http://localhost:8880/entry"
    headers = {"Content-Type": "application/json"}

    for i in range(1, 11):
        payload = {"msg": f"msg{i}"}
        try:
            response = requests.post(facade_url, json=payload, headers=headers, timeout=5)
            if response.status_code in (200, 201):
                print(f"Повідомлення msg{i} відправлено: {response.json()}")
            else:
                print(f"Помилка при відправці msg{i}: {response.status_code} - {response.text}")
        except Exception as e:
            print(f"Помилка при відправці msg{i}: {e}")

if __name__ == "__main__":
    print("Відправка 10 повідомлень...")
    send_messages()
    print("Відправка завершена.")
