from __future__ import print_function

import click
import os

import grpc
import medias_pb2
import medias_pb2_grpc

SERVER_ADDRESS = "localhost:50051"


@click.group()
@click.option('--debug', is_flag=True, help='Enable debug mode.')
@click.pass_context
def cli(ctx, debug):
	ctx.ensure_object(dict)
	ctx.obj['DEBUG'] = debug
	
	

@cli.command()
@click.argument('id', type=int)
@click.pass_context
def get(ctx, id):
	"""Get a single cubeobject with the given ID"""
	if (id) and (id>0):
		if ctx.obj['DEBUG']:
			click.echo(f"DEBUG: Getting element with ID: {id}")
		with grpc.insecure_channel(SERVER_ADDRESS) as channel:
			stub =  medias_pb2_grpc.MediasStub(channel)
			request = medias_pb2.GetMediaByIdRequest(id=id)
			response = stub.GetMediaById(request)
			if response.success:
				click.echo(response.media)
			else:
				click.echo("Error: index not found in database.")
	else:
		click.echo("ERROR: index must be > 0")

@cli.command()
@click.pass_context
@click.option('--attributes', '-a', multiple=True, default=['id', 'file_uri', 'file_type', 'thumbnail_uri'],
	      					help='Attributes to include (default=ALL : id, file_uri, file_type, thumbnail_uri)')
def getall(ctx, attributes):
		"""Get all the cubeobjects stored."""
		if ctx.obj['DEBUG']:
			click.echo("DEBUG: Getting all elements")
		with grpc.insecure_channel(SERVER_ADDRESS) as channel:
			stub =  medias_pb2_grpc.MediasStub(channel)
			request = medias_pb2.GetAllMediasRequest()
			response_iterator = stub.GetAllMedias(request)
			for response in response_iterator:
					for attr in attributes:
						click.echo("%s: %s" % (attr, getattr(response.media, attr)))

@cli.command()
@click.pass_context
@click.argument('directory')
@click.option('--formats', '-f', multiple=True, default=['jpg', 'png', 'bmp', 'mp3', 'wav', 'flac', 'mp4', 'avi'],
                  help='File formats to include (default: jpg, png, bmp, mp3, wav, flac, mp4, avi)')
def add(ctx, directory, formats):
		"""Add files from the specified directory to the database."""
		if ctx.obj['DEBUG']:
			click.echo("DEBUG: Adding cubeobjects from directory")
		file_count = 0
		def add_media_requests_generator(directory: str):
			nonlocal file_count
			for file in os.listdir(directory):
				if any(file.endswith('.' + ext) for ext in formats):
					file_path = os.path.abspath(os.path.join(directory, file))
					if file.lower().endswith(('jpg', 'png', 'bmp')):
						file_type = 0		# Image
					elif file.lower().endswith(('mp3', 'wav', 'flac')):
						file_type = 1		# Audio
					elif file.lower().endswith(('mp4', 'avi')):
						file_type = 2		# Video
					else: 
						file_type = 3		# Other
					file_count += 1
					request = medias_pb2.AddMediaRequest(media={
						"file_uri": file_path,
						"file_type": file_type,
						"thumbnail_uri": file_path
						})
					yield request
		
		with grpc.insecure_channel(SERVER_ADDRESS) as channel:
			stub =  medias_pb2_grpc.MediasStub(channel)
			response_iterator = stub.AddMedia(add_media_requests_generator(directory))
			click.echo("Found %d items to add in the directory." % file_count)
			for response in response_iterator:
				click.echo("[SERVER] %s, added %d items to database" % (response.message, response.count))

if __name__ == '__main__':
	cli(obj={})

# def printAll(stub: medias_pb2_grpc.MediasStub):
#     print("--------------Call GetAllMedias Service--------------")
#     request = medias_pb2.GetAllMediasRequest()
#     response_iterator = stub.GetAllMedias(request)
#     for response in response_iterator:
#         print(response)
#     print("--------------Call ServerStreamingMethod Over---------------")

# def printOne(stub: medias_pb2_grpc.MediasStub, id: int):
#     print("--------------Call GetMediaById Begin--------------")
#     request = medias_pb2.GetMediaByIdRequest(id=id)
#     response = stub.GetMediaById(request)
#     print(response)
#     print("--------------Call SimpleMethod Over---------------")

# def addMedia(stub: medias_pb2_grpc.MediasStub):
# 	print("--------------Call AddMedia Begin--------------")

# 	def add_media_requests_generator():
# 		for i in range(5):
# 			request = medias_pb2.AddMediaRequest(media={
# 					"file_uri":"uri %d" % i,
# 					"file_type": 1,
# 					"thumbnail_uri":"thumbnail%d" % i
# 			})
# 			yield request

# 	response = stub.AddMedia(add_media_requests_generator())
# 	print("%s, added %d items to database" % (response.message, response.count))
# 	print("--------------Call AddMedia Over---------------")

# def runTests():
# 	with grpc.insecure_channel(SERVER_ADDRESS) as channel:
# 		stub = medias_pb2_grpc.MediasStub(channel)

# 		printAll(stub)
# 		print("\n")
# 		printOne(stub,1)
# 		addMedia(stub)

