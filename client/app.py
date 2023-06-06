from __future__ import print_function

import logging
import asyncio
import click

import grpc
import medias_pb2
import medias_pb2_grpc

SERVER_ADDRESS = "localhost:50051"


@click.command()
@click.option('--get', type=int, help='ID of the media to get')
def cli(get):
	with grpc.insecure_channel(SERVER_ADDRESS) as channel:
		stub = medias_pb2_grpc.MediasStub(channel)
		printOne(stub, get)



def printAll(stub: medias_pb2_grpc.MediasStub):
    print("--------------Call GetMedias Service--------------")
    request = medias_pb2.GetMediasRequest()
    response_iterator = stub.GetMedias(request)
    for response in response_iterator:
        print(response)
    print("--------------Call ServerStreamingMethod Over---------------")

def printOne(stub: medias_pb2_grpc.MediasStub, id: int):
    print("--------------Call GetMediaById Begin--------------")
    request = medias_pb2.GetMediaByIdRequest(id=id)
    response = stub.GetMediaById(request)
    print(response)
    print("--------------Call SimpleMethod Over---------------")

def addMedia(stub: medias_pb2_grpc.MediasStub):
	print("--------------Call AddMedia Begin--------------")

	def add_media_requests_generator():
		for i in range(5):
			request = medias_pb2.AddMediaRequest(media={
					"file_uri":"uri %d" % i,
					"file_type": 1,
					"thumbnail_uri":"thumbnail%d" % i
			})
			yield request

	response = stub.AddMedia(add_media_requests_generator())
	print("%s, added %d items to database" % (response.message, response.count))
	print("--------------Call AddMedia Over---------------")

def runTests():
	with grpc.insecure_channel(SERVER_ADDRESS) as channel:
		stub = medias_pb2_grpc.MediasStub(channel)

		printAll(stub)
		print("\n")
		printOne(stub,1)
		addMedia(stub)

if __name__ == '__main__':
    runTests()