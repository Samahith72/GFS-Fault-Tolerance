import grpc

from shared import gfs_pb2
from shared import gfs_pb2_grpc

filename = input(
    "Filename: "
)

channel = grpc.insecure_channel(
    "localhost:5050"
)

stub = gfs_pb2_grpc.MasterServiceStub(
    channel
)

stub.DeleteFile(

    gfs_pb2.FileRequest(
        filename=filename
    )
)

print(
    "Deleted"
)