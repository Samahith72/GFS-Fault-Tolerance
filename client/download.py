import grpc
import os

from shared import gfs_pb2
from shared import gfs_pb2_grpc

MASTER = "localhost:5050"

filename = input(
    "Filename: "
)

channel = grpc.insecure_channel(
    MASTER
)

master = gfs_pb2_grpc.MasterServiceStub(
    channel
)

metadata = master.GetFile(

    gfs_pb2.FileRequest(
        filename=filename
    )
)

if not metadata.chunks:

    print(
        "File not found"
    )

    exit()

os.makedirs(
    "downloads",
    exist_ok=True
)

output_path = os.path.join(
    "downloads",
    filename
)

with open(
    output_path,
    "w"
) as outfile:

    chunk_channel = grpc.insecure_channel(
        "localhost:5001"
    )

    chunk_stub = (
        gfs_pb2_grpc
        .ChunkServiceStub(
            chunk_channel
        )
    )

    for chunk_id in metadata.chunks:

        response = chunk_stub.ReadChunk(

            gfs_pb2.ReadRequest(
                chunk_id=chunk_id
            )
        )

        outfile.write(
            response.data
        )

        print(
            f"Downloaded "
            f"{chunk_id}"
        )

print(
    f"File restored -> "
    f"{output_path}"
)