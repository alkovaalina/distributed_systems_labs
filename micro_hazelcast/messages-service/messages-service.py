from flask import Flask, request, jsonify

stub = Flask(__name__)

@stub.route("/placeholder", methods=["GET"])
def respond():
    return jsonify({"status": "Placeholder active, no features yet"})

if __name__ == "__main__":
    stub.run(host='0.0.0.0', port=8882)
