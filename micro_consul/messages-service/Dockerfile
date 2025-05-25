FROM python:3.10-slim

WORKDIR /app
COPY . .

RUN pip install flask hazelcast-python-client requests kafka-python python-consul

CMD ["python", "messages-service.py"]
