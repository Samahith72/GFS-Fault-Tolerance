import time

class Metadata:

    def __init__(self):

        self.primary = None

        self.nodes = {}

        self.file_table = {}

        self.lease_expiry = (
            time.time() + 10
        )

metadata = Metadata()