# client/status.py

import grpc

from shared import gfs_pb2
from shared import gfs_pb2_grpc

channel = grpc.insecure_channel(
    "localhost:5050"
)

stub = gfs_pb2_grpc.MasterServiceStub(
    channel
)

response = stub.GetStatus(
    gfs_pb2.Empty()
)

print(
    f"Primary: {response.primary}"
)

print(
    f"Lease: {response.lease_remaining}"
)

print(
    f"S1: {response.server1}"
)

print(
    f"S2: {response.server2}"
)

print(
    f"S3: {response.server3}"
)