import rpyc
import sys
import os
import logging

def send_to_storage(block_uuid,data,storage_servers):
    LOG.info("sending: " + str(block_uuid) + str(storage_servers))
    storage=storage_servers[0]
    storage_servers=storage_servers[1:]
    host,port=storage

    con=rpyc.connect(host,port=port, config = {"allow_public_attrs" : True})
    storage = con.root
    storage.write_file(block_uuid,data,storage_servers)


def read_from_storage(block_uuid,storage):
  host,port = storage
  con=con.root(host,port=port, config = {"allow_public_attrs" : True})
  #TODO chech the name
  storage = con.root
  return storage.read_file(block_uuid)


def init():
	return 0

# TODO check the argument
def create_file(name_server, path):
	print("Creating the file ... ")
	name_server.create_file(path) 
	print("File created") 
	return 0


def read_file(name_server, path):
	#TODO check the name of name_server
	file_table = name_server.get_file_table_entry(path)
	if not file_table:
		LOG.info("404: file not found")
		return
	# block[1] - server on which it is stored
    # block[0] - id of the block
	for block in file_table:
		for m in [name_server.get_minions()[_] for _ in block[1]]:
			data = read_from_storage(block[0], m)
			if data:
				sys.stdout.write(data)
				break
			else:
				LOG.info("No blocks found. Possibly a corrupt file")


def write_file(name_server, path, content):
	print("Writing to file")
	size = os.path.getsize(content)
	blocks = name_server.write(path, content)
	with open(data) as f:
		for b in blocks:
			data = f.read(name_server.get_block_size())
			block_uuid=b[0]
			storage_servers = [name_server.get_minions()[_] for _ in b[1]]
			send_to_storage(block_uuid,data,storage_servers)
	print("File changed") 
	return 0

def delete_file(name_server, path):
	print("Deleting the file")
	file_table = name_server.get_file_table_entry(path)
	if not file_table:
		LOG.info("404: file not found")
		return

	for block in file_table:
		for m in [name_server.get_minions()[_] for _ in block[1]]:
			host,port = m
			con=con.root(host,port=port, config = {"allow_public_attrs" : True})
			con.root
			m.delete_file(block[0])
	if data:
		print("File removed")
	else:
		LOG.info("No blocks found. Possibly a corrupt file")
	return 0

def info_file(path):
	print("Information about the file")
	file  = path
	return 0

def copy_file(src, dest):
	print("Copying the file from src to dest ...")
	source_file = src
	destination_file = dest
	print("File copied") 
	return 0

def move_file(src, dest):
	print("Moving the file from src to dest ...")
	source_file = src
	destination_file = dest
	print("File moved") 
	return 0

def open_dir(path):
	print("Changing the directory")
	directory = path
	print("Directory changed") 
	return 0

def read_dir(path):
	print("Listing everything in dir ...")
	directory - path
	return 0

def make_dir(name_server, path):
	print("Creating the dir...")
	name_server.create_file(path)
	print("Diirectory created")
	return 0

def delete_dir(path):
	print("Removing th dir ...")
	directory = path
	print("Dir deleted")
	return 0

con=rpyc.connect("127.0.0.1",port=2000, config = {"allow_public_attrs" : True})
name_server=con.root
menu = sys.argv[1]

if menu == 'init':
	init()
elif menu == 'create_file':
	create_file(name_server,sys.argv[2])
elif menu == 'read_file':
	read_file(name_server, sys.argv[2])
elif menu == 'write_file':
	write_file(name_server, sys.argv[2], sys.argv[3])
elif menu == 'delete_file':
	delete_file(name_server, sys.argv[2])
elif menu == 'info_file':
	info_file(name_server, sys.argv[2])
elif menu == 'copy_file':
	copy_file(name_server, sys.argv[2], sys.argv[3])
elif menu == 'move_file':
	move_file(name_server, sys.argv[2], sys.argv[3])
elif menu == 'open_dir':
	open_dir(name_server, sys.argv[2])
elif menu == 'read_dir':
	read_dir(name_server, sys.argv[2])
elif menu == 'make_dir':
	make_dir(name_server, sys.argv[2])
elif menu == 'delete_dir':
	delete_dir(name_server, sys.argv[2])
else:
  LOG.error("try 'put srcFile destFile OR get file'")






