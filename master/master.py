# master/master.py

from concurrent import futures
import grpc

from shared import gfs_pb2
from shared import gfs_pb2_grpc
#from shared.config import SERVERS


from master.metadata import metadata
from master.heartbeat_monitor import update
import threading
from master.election import elect_primary

from master.heartbeat_monitor import (
    check_servers
)
from master.lease_manager import (
    lease_loop
)
from master.lease_manager import (
    get_remaining
)

from master.metadata_db import (
    init_db,
    load_primary
)

class MasterService(
    gfs_pb2_grpc.MasterServiceServicer
):
    
    def GetNodes(
    self,
    request,
    context
    ):

        response = gfs_pb2.NodeList()

        for node_id, node in metadata.nodes.items():

            if node["status"] == "UP":

                entry = response.nodes.add()

                entry.node_id = node_id

                entry.address = node["address"]

        return response

    def GetStatus(
    self,
    request,
    context
    ):

        return gfs_pb2.StatusResponse(
            primary=metadata.primary,
            server1="N/A",
            server2="N/A",
            server3="N/A",
            lease_remaining=get_remaining(metadata)
        )

    def GetPrimary(self, request, context):

        if metadata.primary in metadata.nodes:

            return gfs_pb2.PrimaryResponse(
                primary_id=metadata.primary,
                primary_address=
                    metadata.nodes[
                        metadata.primary
                    ]["address"]
            )

        return gfs_pb2.PrimaryResponse(
            primary_id="",
            primary_address=""
        )

    def Heartbeat(
    self,
    request,
    context
    ):

        update(request.server_id)

        if request.server_id in metadata.nodes:

            metadata.nodes[
                request.server_id
            ]["status"] = "UP"

        return gfs_pb2.HeartbeatAck(
            status="OK"
        )
    
    def RegisterNode(
        self,
        request,
        context
    ):

        metadata.nodes[
            request.node_id
        ] = {
            "address":
                request.address,

            "status":
                "UP"
        }

        elect_primary(metadata)

        print(
            f"[MASTER] {request.node_id} joined"
        )

        return gfs_pb2.RegisterResponse(
            status="REGISTERED"
        )

def serve():

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10)
    )

    gfs_pb2_grpc.add_MasterServiceServicer_to_server(
        MasterService(),
        server
    )

    server.add_insecure_port("[::]:5050")
    init_db()

    metadata.primary = load_primary()
    server.start()

    print("MASTER RUNNING")
    threading.Thread(
            target=lease_loop,
            args=(metadata,),
            daemon=True
        ).start()
    threading.Thread(
            target=check_servers,
            args=(metadata,),
            daemon=True
        ).start()

    server.wait_for_termination()

if __name__ == "__main__":
    serve()
    