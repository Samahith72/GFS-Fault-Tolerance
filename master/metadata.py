import time

class Metadata:

    def __init__(self):

        self.primary = None

        self.nodes = {}

        self.lease_expiry = (
            time.time() + 10
        )

metadata = Metadata()