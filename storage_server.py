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

# DATA_DIR="/var/storage/"

'''
Example:
To read a file, the client will run

$~ python3 client.py read project.txt
'''

class StorageService(rpyc.Service):

	def exposed_initialize_storage(self):
		for root, dirs, files in os.walk(DATA_DIR):
		    for f in files:
		        os.unlink(os.path.join(root, f))
		    for d in dirs:
		        shutil.rmtree(os.path.join(root, d))

	# def exposed_is_file(self, file_name):
	# 	return os.path.isfile(DATA_DIR + file_name)

	# # done on naming server
	# def exposed_create_file(self, file_name):
	# 	if os.path.exists(DATA_DIR + file_name) == False:
	# 		open(DATA_DIR + file_name, "w").close

    	# try:
	    #     with open(DATA_DIR + str(block_id), "x") as fd:
	    #         fd.close()
	    #         return True
	    # except FileExistsError:
	    #     return False

	def exposed_read_file(self, block_id):
		if not os.path.isfile(DATA_DIR + str(block_id)):
			return None
		else:
			with open(os.path.join(DATA_DIR, str(block_id))) as f:
				return f.read() 

	def exposed_write_file(self, block_id, data, storage_servers):
		# if not os.path.isfile(DATA_DIR + str(block_id)):
		# 	return self.create_file(str(block_id))

		with open(os.path.join(DATA_DIR, str(block_id)), 'w') as f:
			f.write(data)

		if len(storage_servers) > 0:
			self.replicate_write(block_id, data, storage_servers)

	def exposed_delete_file(self, block_id, storage_servers):
		if os.path.exists(DATA_DIR + str(block_id)):
			os.remove(DATA_DIR + str(block_id))
		if len(storage_servers) > 0:
			self.replicate_delete(block_id, storage_servers)

	def exposed_file_info(self, file_name):
		if os.path.exists(DATA_DIR + file_name):
			st = os.stat(DATA_DIR + file_name)
			return st
		else:
			print("File does not exist")

	def exposed_copy_file(self, src_id, dest_id):
		if os.path.exists(DATA_DIR + str(src_id)):
			shutil.copy(DATA_DIR + str(src_id), DATA_DIR + str(dest_id))
			return True
		else:
			return False

	def exposed_move_file(self, old_path, new_path):
		if os.path.exists(DATA_DIR + str(old_path)):
			shutil.move(DATA_DIR + str(old_path), DATA_DIR + str(new_path))
			return True
		else:
			return False

	def exposed_open_dir(self, path):
		os.chdir(DATA_DIR + path)

	def exposed_read_dir(self, path):
		for root, dirs, files in os.walk(DATA_DIR + path, topdown=True):
		   for name in files:
		      print(os.path.join(root, name))
		   for name in dirs:
		      print(os.path.join(root, name))

	# done on naming server
	def exposed_make_dir(self, path):
		try:
		    os.mkdir(DATA_DIR + path)
		except OSError:
		    print ("Creation of the directory %s failed" % DATA_DIR + path)
		else:
		    print ("Successfully created the directory %s " % DATA_DIR + path)

	def exposed_delete_dir(self, path):
		try:
		    os.rmdir(DATA_DIR + path)
		except OSError:
		    print ("Deletion of the directory %s failed" % DATA_DIR + path)
		else:
		    print ("Successfully deleted the directory %s " % DATA_DIR + path)

	def replicate_write(self, block_id, data, storage_servers):
		print("2 here")
		print(storage_servers)
		storage_server = storage_servers[0]
		storage_servers = storage_servers[1:]

		host, port = storage_server

		conn = rpyc.connect(host, port=port, config={"allow_public_attrs" : True})
		storage_server = conn.root
		storage_server.write_file(block_id, data, storage_servers)

	def replicate_delete(block_id, storage_servers):
		storage_server = storage_servers[0]
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
	arg.add_argument("-dir", "--directory", required=True, help="operating directory")
	args = vars(arg.parse_args())
	PORT = int(args['port'])
	DATA_DIR = args['directory']

	if not os.path.isdir(DATA_DIR): os.mkdir(DATA_DIR)

	t = ThreadedServer(StorageService, port=PORT, protocol_config={"allow_public_attrs" : True})
	t.start()