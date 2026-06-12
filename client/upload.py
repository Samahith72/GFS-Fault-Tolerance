import grpc
import os

from shared import gfs_pb2
from shared import gfs_pb2_grpc

MASTER = "localhost:5050"

CHUNK_SIZE = 1024


filename = input(
    "File Path: "
)

base = os.path.basename(
    filename
)

chunks = []

with open(
    filename,
    "rb"
) as f:

    index = 0

    while True:

        data = f.read(
            CHUNK_SIZE
        )

        if not data:
            break

        chunk_name = (
            f"{base}_chunk_{index}"
        )

        chunks.append(
            chunk_name
        )

        channel = grpc.insecure_channel(
            "localhost:5001"
        )

        stub = (
            gfs_pb2_grpc
            .ChunkServiceStub(
                channel
            )
        )

        stub.WriteChunk(
            gfs_pb2.WriteRequest(
                chunk_id=chunk_name,
                data=data.decode(
                    errors="ignore"
                )
            )
        )

        print(
            f"Uploaded "
            f"{chunk_name}"
        )

        index += 1

channel = grpc.insecure_channel(
    MASTER
)

master = (
    gfs_pb2_grpc
    .MasterServiceStub(
        channel
    )
)

master.RegisterFile(

    gfs_pb2.FileMetadata(

        filename=base,

        chunks=chunks
    )
)

print(
    "Upload Complete"
)