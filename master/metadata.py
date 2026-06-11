# master/metadata.py

class Metadata:

    def __init__(self):

        self.primary = "1"

        self.servers = {
            "1": "UP",
            "2": "UP",
            "3": "UP"
        }

metadata = Metadata()