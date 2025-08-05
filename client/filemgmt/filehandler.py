import grpc_client
import logging

class FileHandler:
# Abstract class for file handlers (CSV and JSON)
    def __init__(self) -> None:
        self.client  = grpc_client.LoaderClient()
        logger = logging.getLogger(__name__)
        logging.basicConfig(filename='execution.log', format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S', encoding='utf-8', level=logging.DEBUG)
