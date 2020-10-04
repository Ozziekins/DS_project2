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

from rpyc.utils.server import ThreadedServer

DATA_DIR="/var/storage/"

'''
Example:
To read a file, the client will run

$~ python3 client.py read project.txt
'''

class StorageService(rpyc.Service):
	def exposed_initialize_storage(self):
		shutil.rmtree(DATA_DIR) 

	def exposed_is_file(self, block_id):
		return os.path.isfile(DATA_DIR + str(block_id))

	def exposed_create_file(self, block_id):
		if os.path.exists(DATA_DIR + str(block_id)) == False:
    		open(DATA_DIR + str(block_id), "w").close

    	# try:
	    #     with open(DATA_DIR + str(block_id), "x") as fd:
	    #         fd.close()
	    #         return True
	    # except FileExistsError:
	    #     return False

	def exposed_read_file(self, block_id):
		if not os.path.isfile(DATA_DIR + str(block_id)):
			return None
		with open(os.path.join(DATA_DIR, str(block_id))) as f:
			return f.read() 

	def exposed_write_file(self, block_id, data, storage_servers):
		if not os.path.isfile(DATA_DIR + str(block_id)):
			return self.create_file(block_id)
		with open(os.path.join(DATA_DIR, str(block_id)), 'w') as f:
			return f.write(data)
		if len(storage_servers) > 0:
			self.replicate(block_id, data, storage_servers)

	def exposed_delete_file(self, block_id):
		if os.path.exists(DATA_DIR + str(block_id)):
	        os.remove(DATA_DIR + str(block_id))
	        return True
	    else:
	        return False

	def exposed_file_info(self, block_id):
		if os.path.exists(DATA_DIR + str(block_id)):
			st = os.stat(DATA_DIR + str(block_id))
			return st
		else:
			print("File does not exist")

# rootdir = '/tmp'
# for root, dirs, files in os.walk(rootdir):
#     for fname in files:
#         if fname == 'somefile.txt':
#             with open(os.path.join(root, fname)) as f:
#                 print('Filename: %s' % fname)
#                 print('directory: %s' % root)
#                 print(f.read())


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

	def exposed_open_dir(self, block_id):
		os.chdir(DATA_DIR + str(block_id))

	def exposed_read_dir(self, block_id):
		for root, dirs, files in os.walk(DATA_DIR + str(block_id), topdown = True):
		   for name in files:
		      print(os.path.join(root, name))
		   for name in dirs:
		      print(os.path.join(root, name))

	def exposed_make_dir(self, block_id):
		try:
		    os.mkdir(DATA_DIR + str(block_id))
		except OSError:
		    print ("Creation of the directory %s failed" % DATA_DIR + str(block_id))
		else:
		    print ("Successfully created the directory %s " % DATA_DIR + str(block_id))

	def exposed_delete_dir(self, block_id):
		try:
		    os.rmdir(DATA_DIR + str(block_id))
		except OSError:
		    print ("Deletion of the directory %s failed" % DATA_DIR + str(block_id))
		else:
		    print ("Successfully deleted the directory %s " % DATA_DIR + str(block_id))

	def replicate(self, block_id, data, storage_servers):
		storage_server = storage_servers[0]
		storage_servers = storage_servers[1:]

		host, port = storage_server

		conn = rpyc.connect(host, port=port)
		storage_server = conn.root
		storage_server.write_file(block_id, data, storage_servers)


'''
subnet of storage servers and possibly naming server: 10.0.15.0/24
'''

if __name__ == "__main__":
	if not os.path.isdir(DATA_DIR): os.mkdir(DATA_DIR)
	t = ThreadedServer(StorageService, port = 8888)
	t.start()