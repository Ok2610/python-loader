from concurrent import futures
import logging
import psycopg2
import psycopg2.extras
import sys
import asyncio
from typing import Iterable

import grpc
import medias_pb2
from medias_pb2_grpc import MediasServicer, add_MediasServicer_to_server

MODE = "add"
PACKET_SIZE = 20


class Medias(MediasServicer):
    def __init__(self) -> None:
        super().__init__()
        self.conn = psycopg2.connect(
            database="loader-testing",
            user="postgres",
            password="root",
            host="localhost",
            port="5432",
        )
        self.conn.autocommit = True
        self.cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        self.cursor.execute("select version()")
        data = self.cursor.fetchone()
        print("Connection established to: ", data)
        if MODE == "reset":
            self.cursor.execute(open("init.sql", "r").read())
            print("DB has been reset")

    def __del__(self):
        self.cursor.close()
        self.conn.close()

    def GetMediaById(
        self, request: medias_pb2.GetMediaByIdRequest, context: grpc.ServicerContext
    ):
        print("Received GetMedia request with id: %d" % request.id)
        try:
            sql = """SELECT * FROM cubeobjects WHERE id=%d""" % request.id
            self.cursor.execute(sql)
            result = self.cursor.fetchall()[0]
            print("  -> Element fetched from DB, sending back to client...")
            return medias_pb2.MediaResponse(
                success=True,
                media={
                    "id": result["id"],
                    "file_uri": result["file_uri"],
                    "file_type": result["file_type"],
                    "thumbnail_uri": result["thumbnail_uri"],
                },
            )
        except:
            print("  -> No results were fetched, sending error message to client...")
            return medias_pb2.MediaResponse(success=False)

    def GetAllMedias(
        self, request: medias_pb2.GetAllMediasRequest, context: grpc.ServicerContext
    ):
        print("Received get_all_medias request")
        count = 0
        sql = """SELECT * FROM cubeobjects"""
        self.cursor.execute(sql)
        for row in self.cursor:
            count += 1
            yield medias_pb2.MediaResponse(
                success=True,
                media={
                    "id": row["id"],
                    "file_uri": row["file_uri"],
                    "file_type": row["file_type"],
                    "thumbnail_uri": row["thumbnail_uri"],
                },
            )
        print("Fetched %d items from database" % count)

    def AddMedia(self, request_iterator, context: grpc.ServicerContext):
        print("AddMedia service called by client...")
        request_counter = 0
        sql = ""
        for request in request_iterator:
            request_counter += 1
            if request_counter % PACKET_SIZE == 1:
                sql = """INSERT INTO cubeobjects (file_uri, file_type, thumbnail_uri)
VALUES
"""
            sql += "('%s', %d, '%s')," % (
                request.media.file_uri,
                request.media.file_type,
                request.media.thumbnail_uri,
            )
            if request_counter % PACKET_SIZE == 0:
                sql = sql[:-1] + ";COMMIT;"
                try:
                    self.cursor.execute(sql)
                    response = medias_pb2.AddMediaResponse(
                        message="Processing", count=request_counter
                    )
                except:
                    response = medias_pb2.AddMediaResponse(message="Error", count=0)
                    print("Error: packet addition failed.")
                yield response

        if request_counter % PACKET_SIZE > 0:
            sql = sql[:-1] + ";COMMIT;"
            try:
                self.cursor.execute(sql)
                response = medias_pb2.AddMediaResponse(
                    message="Processing", count=request_counter
                )
            except:
                response = medias_pb2.AddMediaResponse(message="Error", count=0)
                print("Error: packet addition failed.")
            yield response
        print("Operation completed, added %d elements to DB" % request_counter)


def serve() -> None:
    server = grpc.server(futures.ThreadPoolExecutor())
    add_MediasServicer_to_server(Medias(), server)
    listen_addr = "[::]:50051"
    server.add_insecure_port(listen_addr)
    server.start()
    print("Server listening at %s" % listen_addr)
    server.wait_for_termination()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        MODE = sys.argv[1]
    logging.basicConfig(level=logging.INFO)
    serve()
