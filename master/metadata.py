import time

class Metadata:

    def __init__(self):

        self.primary = "1"

        self.servers = {
            "1": "UP",
            "2": "UP",
            "3": "UP"
        }

        self.lease_expiry = time.time() + 10

metadata = Metadata()