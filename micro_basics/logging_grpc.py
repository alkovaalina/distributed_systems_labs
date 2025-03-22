import grpc
from concurrent import futures
import tracker_pb2
import tracker_pb2_grpc

# Словник для зберігання даних
records = {}

class TrackerServicer(tracker_pb2_grpc.TrackerServicer):
    def LogEntry(self, request, context):
        identifier = request.key
        info = request.data

        if not (identifier and info):
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("Missing key or data")
            return tracker_pb2.LogResponse(status="Error")

        # Дедуплікація
        if identifier in records:
            print(f"Repeated entry skipped: {identifier}")
            return tracker_pb2.LogResponse(status="Skipped repeat")

        records[identifier] = info
        print(f"Tracked: {info} [Key: {identifier}]")
        return tracker_pb2.LogResponse(status="Entry recorded")

    def GetEntries(self, request, context):
        return tracker_pb2.EntriesResponse(entries=", ".join(records.values()))

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    tracker_pb2_grpc.add_TrackerServicer_to_server(TrackerServicer(), server)
    server.add_insecure_port('[::]:50051')
    print("gRPC TrackerService running on port 50051...")
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    serve()
