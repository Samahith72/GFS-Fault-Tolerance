import time
from master.election import elect_primary

last_seen = {}


def update(server_id):
    last_seen[server_id] = time.time()


def check_servers(metadata):

    while True:

        now = time.time()

        for sid in metadata.servers:

            if sid not in last_seen:
                continue

            diff = now - last_seen[sid]

            if diff > 6:

                if metadata.servers[sid] == "UP":

                    metadata.servers[sid] = "DOWN"

                    print(
                        f"[MASTER] Server {sid} DOWN"
                    )

                    if sid == metadata.primary:

                        print(
                            "[MASTER] Primary failed"
                        )

                        elect_primary(metadata)

        time.sleep(2)