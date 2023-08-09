import grpc_client

class FileHandler:
    def __init__(self) -> None:
        self.client  = grpc_client.LoaderClient()