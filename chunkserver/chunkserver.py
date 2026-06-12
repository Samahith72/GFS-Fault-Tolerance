import grpc
import threading
import time
import os
import argparse
from concurrent import futures
import socket

from shared import gfs_pb2
from shared import gfs_pb2_grpc


#from shared.config import MASTER_ADDRESS
MASTER_ADDRESS = "localhost:5050"

server_id = None
storage_dir = None
NODE_ID = None

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
                    server_id=NODE_ID
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
def am_i_primary():

    try:

        channel = grpc.insecure_channel(
            MASTER_ADDRESS
        )

        stub = gfs_pb2_grpc.MasterServiceStub(
            channel
        )

        response = stub.GetPrimary(
            gfs_pb2.Empty()
        )

        return response.primary_id == NODE_ID

    except Exception as e:

        print(
            f"[{server_id}] Primary check failed:",
            e
        )

        return False
    
def replicate_to_secondaries(
    chunk_id,
    data
):

    nodes = get_active_nodes()

    for node in nodes:

        if node.node_id == NODE_ID:
            continue

        try:

            channel = grpc.insecure_channel(
                node.address
            )

            stub = (
                gfs_pb2_grpc
                .ChunkServiceStub(
                    channel
                )
            )

            stub.ReplicateChunk(

                gfs_pb2.WriteRequest(

                    chunk_id=chunk_id,

                    data=data
                )
            )

            print(
                f"[{NODE_ID}] "
                f"replicated to "
                f"{node.node_id}"
            )

        except Exception as e:

            print(
                f"Replication failed "
                f"to {node.node_id}: {e}"
            )
def synchronize_from_primary():

    try:

        channel = grpc.insecure_channel(
            MASTER_ADDRESS
        )

        master_stub = gfs_pb2_grpc.MasterServiceStub(
            channel
        )

        primary_response = master_stub.GetPrimary(
            gfs_pb2.Empty()
        )
        print(primary_response)

        if primary_response.primary_id == NODE_ID:
            return

        channel = grpc.insecure_channel(
            primary_response.primary_address
        )

        chunk_stub = gfs_pb2_grpc.ChunkServiceStub(
            channel
        )

        response = chunk_stub.SyncChunk(
            gfs_pb2.ReadRequest(
                chunk_id="chunk1"
            )
        )

        filepath = os.path.join(
            storage_dir,
            "chunk1.txt"
        )

        with open(filepath, "w") as f:
            f.write(response.data)

        print(
            f"[{server_id}] Sync Complete"
        )

    except Exception as e:

        print(
            f"[{server_id}] Sync Failed: {e}"
        )

def register_with_master():

    try:

        channel = grpc.insecure_channel(
            MASTER_ADDRESS
        )

        stub = gfs_pb2_grpc.MasterServiceStub(
            channel
        )

        hostname = socket.gethostname()
        NODE_ID = f"{hostname}-{server_id}"
        #node_id = f"node-{server_id}"

        address = (
            f"localhost:"
            f"{5000 + int(server_id)}"
        )

        response = stub.RegisterNode(

            gfs_pb2.NodeInfo(

                node_id=NODE_ID,

                address=address
            )
        )

        print(response.status)

    except Exception as e:

        print(
            "Registration Failed:",
            e
        )

def get_active_nodes():

    try:

        channel = grpc.insecure_channel(
            MASTER_ADDRESS
        )

        stub = gfs_pb2_grpc.MasterServiceStub(
            channel
        )

        response = stub.GetNodes(
            gfs_pb2.Empty()
        )

        return response.nodes

    except Exception as e:

        print(
            f"GetNodes failed: {e}"
        )

        return []


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

        if am_i_primary():

            print(
              f"[{server_id}] I am primary"
            )

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
    
    def SyncChunk(
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
    
    
    

def serve():

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
        register_with_master()

        time.sleep(2)
        synchronize_from_primary()

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

    hostname = socket.gethostname()

    NODE_ID = f"{hostname}-{server_id}"

    storage_dir = f"storage/server{server_id}"

    os.makedirs(
        storage_dir,
        exist_ok=True
    )

    serve()
    
