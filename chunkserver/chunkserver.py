import grpc
import threading
import time
import os
import argparse
from concurrent import futures

from shared import gfs_pb2
from shared import gfs_pb2_grpc
from shared.config import SERVERS


MASTER_ADDRESS = "localhost:5050"

server_id = None
storage_dir = None

def heartbeat_loop():

    while True:

        try:

            channel = grpc.insecure_channel(
                MASTER_ADDRESS
            )

            stub = gfs_pb2_grpc.MasterServiceStub(
                channel
            )

            stub.Heartbeat(
                gfs_pb2.ServerInfo(
                    server_id=server_id
                )
            )

            print(
                f"[{server_id}] Heartbeat Sent"
            )

        except Exception as e:

            print(
                f"[{server_id}] Heartbeat Error:",
                e
            )

        time.sleep(2)

def replicate_to_secondaries(chunk_id, data):

    for sid, address in SERVERS.items():

        if sid == server_id:
            continue

        try:

            channel = grpc.insecure_channel(address)

            stub = gfs_pb2_grpc.ChunkServiceStub(
                channel
            )

            response = stub.ReplicateChunk(
                gfs_pb2.WriteRequest(
                    chunk_id=chunk_id,
                    data=data
                )
            )

            print(
                f"[{server_id}] replicated to {sid}"
            )

        except Exception as e:

            print(
                f"Replication failed to {sid}: {e}"
            )

class ChunkService(
    gfs_pb2_grpc.ChunkServiceServicer
):
            
    def WriteChunk(
        self,
        request,
        context
    ):

        filepath = os.path.join(
           storage_dir,
            f"{request.chunk_id}.txt"
        )

        with open(filepath, "a") as f:
            f.write(request.data + "\n")

        if server_id == "1":
            replicate_to_secondaries(
                request.chunk_id,
                request.data
            )

        print(
            f"[{server_id}] WRITE:",
            request.data
        )

        return gfs_pb2.WriteResponse(
            status="SUCCESS"
        )

    def ReadChunk(
        self,
        request,
        context
    ):

        filepath = os.path.join(
            storage_dir,
            f"{request.chunk_id}.txt"
        )

        if not os.path.exists(filepath):

            return gfs_pb2.ReadResponse(
                data=""
            )

        with open(filepath) as f:

            data = f.read()

        return gfs_pb2.ReadResponse(
            data=data
        )

    def ReplicateChunk(
        self,
        request,
        context
    ):

        filepath = os.path.join(
            storage_dir,
            f"{request.chunk_id}.txt"
        )

        with open(filepath, "a") as f:

            f.write(request.data + "\n")

        print(
            f"[{server_id}] REPLICATED:",
            request.data
        )

        return gfs_pb2.WriteResponse(
            status="REPLICATED"
        )

def server():

        grpc_server = grpc.server(
            futures.ThreadPoolExecutor(
                max_workers=10
            )
        )

        gfs_pb2_grpc.add_ChunkServiceServicer_to_server(
            ChunkService(),
            grpc_server
        )

        port = 5000 + int(server_id)

        grpc_server.add_insecure_port(
            f"[::]:{port}"
        )

        grpc_server.start()

        print(
            f"ChunkServer {server_id}"
        )

        threading.Thread(
            target=heartbeat_loop,
            daemon=True
        ).start()

        grpc_server.wait_for_termination()

if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--id",
        required=True
    )

    args = parser.parse_args()

    server_id = args.id

    storage_dir = f"storage/server{server_id}"

    os.makedirs(
        storage_dir,
        exist_ok=True
    )

    server()
    
