from concurrent import futures
import logging
import psycopg2
import psycopg2.extras
import sys
import asyncio

import grpc
from users_pb2 import *
from users_pb2_grpc import UsersServicer, add_UsersServicer_to_server

MODE = "add"

class Users(UsersServicer):
	def __init__(self) -> None:
		super().__init__()
		self.conn = psycopg2.connect(
		database="users", user='postgres', password='root', host='localhost', port='5432'
		)
		self.conn.autocommit = True
		self.cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
		self.cursor.execute("select version()")
		data = self.cursor.fetchone()
		print("Connection established to: ", data)
		if MODE == "reset":
			self.cursor.execute("DROP TABLE IF EXISTS users")
			self.cursor.execute(open("init.sql","r").read())
		
	def __del__(self):
		self.cursor.close()
		self.conn.close()

	async def GetUsers(self, request: GetUsersRequest, context: grpc.aio.ServicerContext) -> UserResponse:
		print("Received get_users request")
		sql = '''SELECT * FROM users'''
		self.cursor.execute(sql)
		result = self.cursor.fetchall()
		for row in result:
			yield UserResponse(user={
				"id":str(row['id']),
				"email":row['email'],
				"name":row['name'],
				"password":row['password']
			})

		
async def serve():
	server = grpc.aio.server()
	add_UsersServicer_to_server(Users(), server)
	listen_addr = "[::]:50051"
	server.add_insecure_port(listen_addr)
	logging.info("Starting server on %s", listen_addr)
	await server.start()
	await server.wait_for_termination()


if __name__ == '__main__':
	MODE = sys.argv[1]
	logging.basicConfig(level=logging.INFO)
	asyncio.run(serve())
