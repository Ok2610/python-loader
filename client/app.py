from __future__ import print_function

import logging
import asyncio

import grpc
import users_pb2
import users_pb2_grpc


async def run() -> None:
    async with grpc.aio.insecure_channel("localhost:50051") as channel:
        stub = users_pb2_grpc.UsersStub(channel)

        # Read from an async generator
        async for response in stub.GetUsers(
            users_pb2.GetUsersRequest()):
            print(response)

        # Direct read from the stub
        users_stream = stub.GetUsers(
            users_pb2.GetUsersRequest())
        while True:
            response = await users_stream.read()
            if response == grpc.aio.EOF:
                break
            print(response)


if __name__ == "__main__":
    logging.basicConfig()
    asyncio.run(run())