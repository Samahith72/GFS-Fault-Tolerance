# master/lease_manager.py

import time

lease_expiry = 0

def start_lease():

    global lease_expiry

    lease_expiry = time.time() + 10

def lease_remaining():

    return max(
        0,
        int(lease_expiry - time.time())
    )