import grpc

from shared import gfs_pb2
from shared import gfs_pb2_grpc
import os

filename = input(
    "Filename: "
)



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

stub.DeleteFile(

    gfs_pb2.FileRequest(
        filename=filename
    )
)

print(
    "Deleted"
)