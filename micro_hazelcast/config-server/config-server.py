from flask import Flask, request, jsonify
import json
import sys

app = Flask(__name__)

registry = {}

@app.route("/services/<name>", methods=["GET"])
def get_service(name):
    if name in registry:
        return jsonify(registry[name])
    return jsonify({"error": "Service not found"}), 404

@app.route("/services", methods=["GET"])
def list_services():
    return jsonify(registry)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python config-server.py services.json")
        sys.exit(1)
    config_path = sys.argv[1]
    with open(config_path, "r") as f:
        registry = json.load(f)
    app.run(host='0.0.0.0', port=8888)
