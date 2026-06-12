from master.metadata_db import save_primary

def elect_primary(metadata):

    alive = []

    for sid, status in metadata.servers.items():

        if status == "UP":
            alive.append(int(sid))

    if not alive:
        return

    new_primary = str(max(alive))

    if metadata.primary != new_primary:

        metadata.primary = new_primary
        save_primary(new_primary)

        print(
            f"[ELECTION] New Primary = {new_primary}"
        )