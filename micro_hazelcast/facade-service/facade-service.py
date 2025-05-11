import random
import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

CONFIG_SERVER_URL = "http://config-server:8888"

def get_service_urls(service_name):
    try:
        response = requests.get(f"{CONFIG_SERVER_URL}/services/{service_name}", timeout=2)
        return response.json() if response.status_code == 200 else []
    except Exception as e:
        print(f"[Facade] Error contacting config-server: {e}")
        return []

def send_to_random_instance(service_name, path, method="GET", data=None):
    urls = get_service_urls(service_name)
    random.shuffle(urls)
    for base_url in urls:
        try:
            url = f"{base_url}{path}"
            if method == "POST":
                r = requests.post(url, json=data, timeout=3)
            else:
                r = requests.get(url, timeout=3)
            if r.status_code in (200, 201):
                print(f"[Facade] Got {service_name} response: {r.json()}")
                return r.json() if "application/json" in r.headers.get("Content-Type", "") else r.text
        except Exception as e:
            print(f"[Facade] WARNING: Fail {url}: {e}")
    return {"error": "All service instances failed"}, 500

@app.route("/entry", methods=["GET", "POST"])
def handle_entry():
    if request.method == "POST":
        msg = request.json.get("msg")
        return send_to_random_instance("logging-service", "/tracker", method="POST", data={"msg": msg})
    else:
        return send_to_random_instance("logging-service", "/tracker", method="GET")

@app.route("/notify", methods=["GET"])
def notify():
    return send_to_random_instance("messages-service", "/placeholder", method="GET")

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8880)
