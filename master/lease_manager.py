import time

def lease_loop(metadata):

    while True:

        metadata.lease_expiry = (
            time.time() + 10
        )

        print(
            f"[LEASE] Renewed for Primary {metadata.primary}"
        )

        time.sleep(10)

def get_remaining(metadata):

    remaining = int(
        metadata.lease_expiry -
        time.time()
    )

    return max(0, remaining)