# master/master.py

from concurrent import futures
import grpc

from shared import gfs_pb2
from shared import gfs_pb2_grpc
from shared.config import SERVERS


from master.metadata import metadata
from master.heartbeat_monitor import update
import threading

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
    def GetStatus(
    self,
    request,
    context
    ):

        return gfs_pb2.StatusResponse(

            primary=metadata.primary,

            server1=metadata.servers["1"],

            server2=metadata.servers["2"],

            server3=metadata.servers["3"],

            lease_remaining=
                get_remaining(metadata)
        )

    def GetPrimary(self, request, context):

        return gfs_pb2.PrimaryResponse(
            primary_id=metadata.primary,
            primary_address=SERVERS[metadata.primary]
        )

    def Heartbeat(self, request, context):

        update(request.server_id)

        metadata.servers[
            request.server_id
        ] = "UP"

        print( f"Heartbeat from {request.server_id}")

        return gfs_pb2.HeartbeatAck(
            status="OK"
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
    threading.Thread(
            target=check_servers,
            args=(metadata,),
            daemon=True
        ).start()
    threading.Thread(
            target=lease_loop,
            args=(metadata,),
            daemon=True
        ).start()
    init_db()
    
    metadata.primary = load_primary()

    server.start()

    print("MASTER RUNNING")

    server.wait_for_termination()

if __name__ == "__main__":
    serve()
    