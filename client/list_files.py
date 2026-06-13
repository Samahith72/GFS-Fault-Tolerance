import grpc

from shared import gfs_pb2
from shared import gfs_pb2_grpc

import os

MASTER = os.getenv(
    "MASTER_ADDRESS",
    "localhost:5050"
)

channel = grpc.insecure_channel(
    MASTER
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