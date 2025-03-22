from flask import Flask, request, jsonify

stub = Flask(__name__)

@stub.route("/placeholder", methods=["GET"])
def respond():
    if request.method == "GET":
        return "Placeholder active, no features yet"
    else:
        return jsonify({"error": "Only GET allowed"}), 405

if __name__ == "__main__":
    stub.run(port=8882)
