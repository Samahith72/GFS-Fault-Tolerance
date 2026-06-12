import grpc

from shared import gfs_pb2
from shared import gfs_pb2_grpc

channel = grpc.insecure_channel(
    "localhost:5050"
)

stub = gfs_pb2_grpc.MasterServiceStub(
    channel
)

response = stub.ListFiles(
    gfs_pb2.Empty()
)

print("\nFILES\n")

for file in response.files:

    print(file)