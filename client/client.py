import grpc

from shared import gfs_pb2
from shared import gfs_pb2_grpc

MASTER = "localhost:5050"


def get_primary():

    channel = grpc.insecure_channel(MASTER)

    stub = gfs_pb2_grpc.MasterServiceStub(
        channel
    )

    response = stub.GetPrimary(
        gfs_pb2.Empty()
    )

    return response.primary_address


def write_data():

    data = input("Data: ")

    primary = get_primary()

    print(
        f"Primary -> {primary}"
    )

    channel = grpc.insecure_channel(
        primary
    )

    stub = gfs_pb2_grpc.ChunkServiceStub(
        channel
    )

    response = stub.WriteChunk(
        gfs_pb2.WriteRequest(
            chunk_id="chunk1",
            data=data
        )
    )

    print(response.status)


while True:

    print("\n1. Write")
    print("2. Exit")

    choice = input("> ")

    if choice == "1":
        write_data()

    elif choice == "2":
        break