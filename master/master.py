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

from master.file_metadata_db import (
    init_file_db,
    save_file,
    load_files,
    delete_file
)

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
    load_primary,
    save_primary
)
class MasterService(
    gfs_pb2_grpc.MasterServiceServicer
):
    
    def DeleteFile(
        self,
        request,
        context
    ):

        filename = request.filename

        if filename not in metadata.file_table:

            return gfs_pb2.StatusResponse(
                primary="NOT_FOUND"
            )

        chunks = metadata.file_table[
            filename
        ]

        for node in metadata.nodes.values():

            try:

                channel = grpc.insecure_channel(
                    node["address"]
                )

                stub = (
                    gfs_pb2_grpc
                    .ChunkServiceStub(
                        channel
                    )
                )

                for chunk in chunks:

                    stub.DeleteChunk(

                        gfs_pb2.ReadRequest(
                            chunk_id=chunk
                        )
                    )

            except Exception as e:

                print(
                    f"Delete failed: {e}"
                )

        del metadata.file_table[
            filename
        ]
        delete_file(
            filename
        )

        print(
            f"[MASTER] Deleted "
            f"{filename}"
        )

        return gfs_pb2.StatusResponse(
            primary="OK"
        )
    
    def ListFiles(
        self,
        request,
        context
    ):

        return gfs_pb2.FileList(

            files=list(
                metadata.file_table.keys()
            )
        )
        
    def RegisterFile(
        self,
        request,
        context
    ):

        metadata.file_table[
            request.filename
        ] = list(
            request.chunks
        )

        save_file(
            request.filename,
            request.chunks
        )

        print(
            f"[MASTER] Registered "
            f"{request.filename}"
        )

        return gfs_pb2.StatusResponse(
            primary="OK"
        )
    
    def GetFile(
        self,
        request,
        context
    ):

        chunks = metadata.file_table.get(
            request.filename,
            []
        )

        return gfs_pb2.FileMetadata(
            filename=request.filename,
            chunks=chunks
        )
    
    def MasterHeartbeat(
        self,
        request,
        context
    ):

        return gfs_pb2.HeartbeatAck(
            status="MASTER_ALIVE"
        )
    

        
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

    def GetPrimary(
        self,
        request,
        context
    ):

        print(
            "PRIMARY:",
            metadata.primary
        )

        print(
            "NODES:",
            metadata.nodes.keys()
        )

        if (
            metadata.primary in metadata.nodes
            and metadata.nodes[
                metadata.primary
            ]["status"] == "UP"
        ):

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

        print(
            f"[MASTER] Heartbeat from "
            f"{request.server_id}"
        )

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

        metadata.nodes[request.node_id] = {
            "address": request.address,
            "status": "UP"
        }

        print(
            f"[MASTER] {request.node_id} joined"
        )

        if metadata.primary is None:

            metadata.primary = request.node_id

            save_primary(metadata.primary)

            print(
                f"[MASTER] Initial Primary = "
                f"{metadata.primary}"
            )

        elif ( metadata.primary not in metadata.nodes  or metadata.nodes.get(  metadata.primary,{} ).get("status") != "UP"):

            metadata.primary = request.node_id

            save_primary(metadata.primary)

            print(
                f"[MASTER] Recovered Primary = "
                f"{metadata.primary}"
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
    if metadata.primary == "":
     metadata.primary = None

    print(
        f"[MASTER] Loaded Primary: {metadata.primary}"
    )
    init_file_db()

    metadata.file_table = (
        load_files()
    )

    print(
        "[MASTER] Files Loaded:",
        metadata.file_table.keys()
    )
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
    