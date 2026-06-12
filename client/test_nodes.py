import grpc

from shared import gfs_pb2
from shared import gfs_pb2_grpc

channel = grpc.insecure_channel(
    "localhost:5050"
)

stub = gfs_pb2_grpc.MasterServiceStub(
    channel
)

response = stub.GetNodes(
    gfs_pb2.Empty()
)

for node in response.nodes:

    print(
        node.node_id,
        node.address
    )