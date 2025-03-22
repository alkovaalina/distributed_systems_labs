from flask import Flask, request, jsonify

tracker = Flask(__name__)

# Словник для зберігання даних
records = {}

@tracker.route("/tracker", methods=["POST", "GET"])
def handle():
    if request.method == "POST":
        identifier = request.form.get("key")
        info = request.form.get("data")

        if not (identifier and info):
            return jsonify({"error": "Missing key or data"}), 400

        # Дедуплікація
        if identifier in records:
            print(f"Repeated entry skipped: {identifier}")
            return jsonify({"status": "Skipped repeat"}), 201

        records[identifier] = info
        print(f"Tracked: {info} [Key: {identifier}]")
        return jsonify({"status": "Entry recorded"}), 201

    elif request.method == "GET":
        return ", ".join(records.values())  # Об'єднуємо дані комами

    else:
        return jsonify({"error": "Method unsupported"}), 405

if __name__ == "__main__":
    tracker.run(port=8881)
