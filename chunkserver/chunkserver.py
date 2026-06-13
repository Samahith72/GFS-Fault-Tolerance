import grpc
import threading
import time
import os
import argparse
from concurrent import futures
import socket

from shared import gfs_pb2
from shared import gfs_pb2_grpc
global KNOWN_NODES



MASTER_MISSES = 0

IS_MASTER = False

KNOWN_NODES = set()

KNOWN_NODE_ADDR = {}
local_master_server = None

import os

'''MASTER_ADDRESS = os.getenv(
    "MASTER_ADDRESS",
    "localhost:5050"
)'''

#MASTER_ADDRESS = "master:5050"
MASTER_ADDRESS = "10.225.7.186:5050"

def set_master(addr):

    global MASTER_ADDRESS

    MASTER_ADDRESS = addr

    print(
        f"[{NODE_ID}] New Master = "
        f"{MASTER_ADDRESS}"
    )

server_id = None
storage_dir = None
NODE_ID = None
#SERVER_NUM = int(server_id)

def heartbeat_loop():

    while True:

        if IS_MASTER:

            time.sleep(2)

            continue

        try:

            channel = grpc.insecure_channel(
                MASTER_ADDRESS
            )

            stub = gfs_pb2_grpc.MasterServiceStub(
                channel
            )

            print(
                f"[{NODE_ID}] Heartbeat Sent"
            )

            stub.Heartbeat(
                gfs_pb2.ServerInfo(
                    server_id=NODE_ID
                )
            )

        except Exception as e:

            print(
                f"[{NODE_ID}] Heartbeat Error:",
                e
            )

        time.sleep(2)

def monitor_master():

    global MASTER_MISSES

    while True:

        if IS_MASTER:

            time.sleep(2)

            continue

        try:

            channel = grpc.insecure_channel(
                MASTER_ADDRESS
            )

            stub = gfs_pb2_grpc.MasterServiceStub(
                channel
            )

            stub.MasterHeartbeat(
                gfs_pb2.Empty()
            )

            MASTER_MISSES = 0

        except Exception:

            MASTER_MISSES += 1

            print(
                f"[{NODE_ID}] "
                f"Master Miss "
                f"{MASTER_MISSES}"
            )

            if MASTER_MISSES >= 4:

                print(
                    f"[{NODE_ID}] "
                    f"MASTER FAILURE DETECTED"
                )

                elect_new_master()

                MASTER_MISSES = 0

        time.sleep(2)

def elect_new_master():

    global MASTER_ADDRESS

    print(
        f"[{NODE_ID}] Starting election"
    )

    alive = {
        "node1",
        "node2",
        "node3"
    }

    print(
        f"[{NODE_ID}] Known Nodes = {alive}"
    )

    winner = sorted(alive)[-1]

    print(
        f"[ELECTION] Winner = {winner}"
    )

    if winner == NODE_ID:

        print(
            f"[{NODE_ID}] "
            f"I AM THE NEW MASTER"
        )
        become_master()

        my_ip = KNOWN_NODE_ADDR[
            NODE_ID
        ].split(":")[0]

        set_master(
            f"{my_ip}:5050"
        )

    else:

        winner_port = KNOWN_NODE_ADDR[
            winner
        ]

        master_addr = (
            winner_port
            .replace(
                ":5001",
                ":5050"
            )
            .replace(
                ":5002",
                ":5050"
            )
            .replace(
                ":5003",
                ":5050"
            )
        )

        set_master(
            master_addr
        )

        print(
            f"[{NODE_ID}] "
            f"Following {master_addr}"
        )

def membership_refresh_loop():

    while True:

        if IS_MASTER:

            time.sleep(5)

            continue

        get_active_nodes()

        print(
            f"[{NODE_ID}] MEMBERSHIP:"
        )

        print(
            KNOWN_NODES
        )

        time.sleep(5)

class LocalMasterService(
    gfs_pb2_grpc.MasterServiceServicer
):

    def MasterHeartbeat(
        self,
        request,
        context
    ):

        return gfs_pb2.HeartbeatAck(
            status="MASTER_ALIVE"
        )

    def GetPrimary(
        self,
        request,
        context
    ):

        return gfs_pb2.PrimaryResponse(
            primary_id=NODE_ID,
            primary_address=
                KNOWN_NODE_ADDR[
                    NODE_ID
                ]
        )

    def GetNodes(
        self,
        request,
        context
    ):

        response = gfs_pb2.NodeList()

        for node_id, address in KNOWN_NODE_ADDR.items():

            entry = response.nodes.add()

            entry.node_id = node_id

            entry.address = address

        return response

    def Heartbeat(
        self,
        request,
        context
    ):

        return gfs_pb2.HeartbeatAck(
            status="OK"
        )


def become_master():

    global local_master_server

    print(
        f"[{NODE_ID}] "
        f"PROMOTING TO MASTER"
    )

    local_master_server = grpc.server(
        futures.ThreadPoolExecutor(
            max_workers=10
        )
    )

    gfs_pb2_grpc.add_MasterServiceServicer_to_server(
        LocalMasterService(),
        local_master_server
    )

    local_master_server.add_insecure_port(
        "[::]:5050"
    )

    local_master_server.start()

    print(
        f"[{NODE_ID}] "
        f"MASTER STARTED ON PORT 5050"
    )

    print(
        f"[{NODE_ID}] "
        f"MASTER STARTED ON PORT 5050"
    )

    my_ip = KNOWN_NODE_ADDR[
        NODE_ID
    ].split(":")[0]

    set_master(
        f"{my_ip}:5050"
    )

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

        if not node.address:
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
                f"Replicated -> "
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

        if not primary_response.primary_address:

            print(
                f"[{NODE_ID}] No valid primary yet"
            )

            return

        channel = grpc.insecure_channel(
            primary_response.primary_address
        )

        chunk_stub = (
            gfs_pb2_grpc
            .ChunkServiceStub(channel)
        )

        chunk_list = chunk_stub.ListChunks(
            gfs_pb2.Empty()
        )

        if not chunk_list.chunks:

            print(
                f"[{NODE_ID}] No chunks to sync"
            )

            return

        for chunk_id in chunk_list.chunks:

            response = chunk_stub.SyncChunk(
                gfs_pb2.ReadRequest(
                    chunk_id=chunk_id
                )
            )

            filepath = os.path.join(
                storage_dir,
                f"{chunk_id}.txt"
            )

            with open(filepath, "w") as f:
                f.write(response.data)

            print(
                f"[{NODE_ID}] Synced {chunk_id}"
            )

        print(
            f"[{NODE_ID}] Sync Complete"
        )

    except Exception as e:

        print(
            f"[{NODE_ID}] Sync Failed: {e}"
        )

def register_with_master():

    global KNOWN_NODE_ADDR
    global KNOWN_NODES

    try:

        channel = grpc.insecure_channel(
            MASTER_ADDRESS
        )

        stub = gfs_pb2_grpc.MasterServiceStub(
            channel
        )

        NODE_IPS = {
            "1": "10.225.7.117",
            "2": "10.225.7.138",
            "3": "10.225.7.254"
        }

        address = (
            f"{NODE_IPS[server_id]}:"
            f"{5000 + int(server_id)}"
        )

        KNOWN_NODE_ADDR = {
            "node1": "10.225.7.117:5001",
            "node2": "10.225.7.138:5002",
            "node3": "10.225.7.254:5003"
        }

        KNOWN_NODES = {
            "node1",
            "node2",
            "node3"
        }

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

    global KNOWN_NODES
    global KNOWN_NODE_ADDR

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

        for node in response.nodes:

            KNOWN_NODES.add(
                node.node_id
            )

            KNOWN_NODE_ADDR[
                node.node_id
            ] = node.address

        return response.nodes

    except Exception as e:

        print(
            f"GetNodes failed: {e}"
        )

        return []


class ChunkService(
    gfs_pb2_grpc.ChunkServiceServicer
):
    
    def DeleteChunk(
        self,
        request,
        context
    ):

        filepath = os.path.join(
            storage_dir,
            f"{request.chunk_id}.txt"
        )

        if os.path.exists(
            filepath
        ):

            os.remove(
                filepath
            )

            print(
                f"[{NODE_ID}] Deleted "
                f"{request.chunk_id}"
            )

        return gfs_pb2.WriteResponse(
            status="DELETED"
        )
            
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
    def ListChunks(
        self,
        request,
        context
    ):

        chunks = []

        for file in os.listdir(storage_dir):

            if file.endswith(".txt"):

                chunks.append(
                    file.replace(
                        ".txt",
                        ""
                    )
                )

        return gfs_pb2.ChunkList(
            chunks=chunks
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
        get_active_nodes()
        time.sleep(2)
        synchronize_from_primary()

        print(
            f"ChunkServer {server_id}"
        )

        threading.Thread(
            target=heartbeat_loop,
            daemon=True
        ).start()
        threading.Thread(
            target=monitor_master,
            daemon=True
        ).start()
        threading.Thread(
            target=membership_refresh_loop,
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

    NODE_ID = f"node{server_id}"

    storage_dir = f"storage/server{server_id}"

    os.makedirs(
        storage_dir,
        exist_ok=True
    )

    serve()
    
