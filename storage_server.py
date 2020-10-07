import rpyc
import uuid
import os
import re 
import sys
import time
import socket
import glob
import pickle
import shutil
import logging
import argparse

logging.basicConfig(level=logging.DEBUG)
LOG = logging.getLogger(__name__)


from rpyc.utils.server import ThreadedServer

class StorageService(rpyc.Service):
	root_dir = "/tmp/storage/"
	data_directory = root_dir

	def exposed_initialize_storage(self):
		self.__class__.data_directory = self.__class__.root_dir
		for root, dirs, files in os.walk(self.__class__.data_directory):
		    for f in files:
		        os.unlink(os.path.join(root, f))
		    for d in dirs:
		        shutil.rmtree(os.path.join(root, d))

	def exposed_read_file(self, block_id):
		if not os.path.isfile(self.__class__.data_directory + str(block_id)):
			return None
		else:
			with open(os.path.join(self.__class__.data_directory, str(block_id))) as f:
				return f.read() 

	def exposed_write_file(self, block_id, data, storage_servers):

		with open(os.path.join(self.__class__.data_directory, str(block_id)), 'w') as f:
			f.write(data)

		if len(storage_servers) > 0:
			self.replicate_write(block_id, data, storage_servers)

	def exposed_delete_file(self, block_id, storage_servers):
		print(self.__class__.data_directory, block_id)
		if os.path.exists(self.__class__.data_directory + str(block_id)):
			os.remove(self.__class__.data_directory + str(block_id))
		if len(storage_servers) > 0:
			self.replicate_delete(block_id, storage_servers)

	def exposed_file_info(self, file_name):
		if os.path.exists(self.__class__.data_directory + file_name):
			st = os.stat(self.__class__.data_directory + file_name)
			return st
		else:
			print("File does not exist")

	def exposed_copy_file(self, src_id, dest_id):
		if os.path.exists(self.__class__.data_directory + str(src_id)):
			shutil.copy(self.__class__.data_directory + str(src_id), self.__class__.data_directory + str(dest_id))
			return True
		else:
			return False

	def exposed_move_file(self, old_path, new_path):
		if os.path.exists(self.__class__.data_directory + str(old_path)):
			shutil.move(self.__class__.data_directory + str(old_path), self.__class__.data_directory + str(new_path))
			return True
		else:
			return False

	def exposed_open_dir(self, path):
		self.__class__.data_directory = self.__class__.data_directory + path
		os.chdir(self.__class__.data_directory)

	def exposed_open_root(self):
		self.__class__.data_directory = self.__class__.root_dir
		os.chdir(self.__class__.data_directory)

	def exposed_read_dir(self, path):
		ls = list()
		for root, dirs, files in os.walk(self.__class__.data_directory + path, topdown=True):
			for name in files:
				ls.append(os.path.join(root, name))
			for name in dirs:
				ls.append(os.path.join(root, name))
		return ls

	def exposed_make_dir(self, path):
		os.mkdir(self.__class__.data_directory + path)

	def exposed_delete_dir(self, path):
		shutil.rmtree(self.__class__.data_directory + path)

	def replicate_write(self, block_id, data, storage_servers):
		storage_server = storage_servers[0]
		storage_servers = storage_servers[1:]

		host, port = storage_server

		conn = rpyc.connect(host, port=port, config={"allow_public_attrs" : True})
		storage_server = conn.root
		storage_server.write_file(block_id, data, storage_servers)

	def replicate_delete(self, block_id, storage_servers):
		print("from storage")
		storage_server = storage_servers[0]
		print(storage_server)
		storage_servers = storage_servers[1:]

		host, port = storage_server

		conn = rpyc.connect(host, port=port, config={"allow_public_attrs" : True})
		storage_server = conn.root
		storage_server.delete_file(block_id, storage_servers)


'''
subnet of storage servers and possibly naming server: 10.0.15.0/24
'''

if __name__ == "__main__":
	arg = argparse.ArgumentParser()
	arg.add_argument("-p", "--port", required=True, help="port number needed")
	args = vars(arg.parse_args())
	PORT = int(args['port'])
	if not os.path.isdir("/tmp/storage/"): os.mkdir("/tmp/storage/")

	t = ThreadedServer(StorageService, port=PORT, protocol_config={"allow_public_attrs" : True})
	t.start()