from master.metadata_db import save_primary

def elect_primary(metadata):

    alive_nodes = []

    for node_id, node in metadata.nodes.items():

        if node["status"] == "UP":

            alive_nodes.append(node_id)

    print(
        "[ELECTION] Alive:",
        alive_nodes
    )

    if not alive_nodes:
        return

    new_primary = sorted(alive_nodes)[-1]

    if metadata.primary != new_primary:

        metadata.primary = new_primary

        save_primary(new_primary)

        print(
            f"[ELECTION] New Primary = {new_primary}"
        )