import time
from master.election import elect_primary

last_seen = {}


def update(server_id):
    last_seen[server_id] = time.time()


def check_servers(metadata):

    while True:

        now = time.time()

        for node_id in list(metadata.nodes.keys()):

            if node_id not in last_seen:
                continue

            diff = now - last_seen[node_id]

            if diff > 6:

                if metadata.nodes[node_id]["status"] == "UP":

                    metadata.nodes[node_id]["status"] = "DOWN"

                    print(
                        f"[MASTER] {node_id} DOWN"
                    )

                    if node_id == metadata.primary:

                        print(
                            "[MASTER] Primary failed"
                        )

                        elect_primary(metadata)

        time.sleep(2)