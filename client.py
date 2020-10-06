import rpyc
import sys
import os
import logging

logging.basicConfig(level=logging.DEBUG)
LOG = logging.getLogger(__name__)

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
	con=rpyc.connect(host,port=port, config = {"allow_public_attrs" : True})
	#TODO chech the name
	storage = con.root
	return storage.read_file(block_uuid)

def initialize_storage_server(storage_server):
    host,port = storage_server
    con=rpyc.connect(host,port=port, config = {"allow_public_attrs" : True})
    storage_server = con.root
    return storage_server.initialize_storage()
 
 # delete from name sever 
def initialize(naming_server):
	available_size = naming_server.initialize()
	storage_servers = naming_server.get_storage_servers()
	for i in range(len(storage_servers)):
		m = storage_servers['{}'.format(i+1)]
		initialize_storage_server(m)
	sys.stdout.write(f'{available_size} bytes available\n')

# TODO check the argument
def create_file(name_server, path):
	name_server.create_file(path, 0)
	return 0


def read_file(name_server, path):
	#TODO check the name of name_server
	file_table = name_server.read(path)
	if not file_table:
		LOG.info("File is empty or doesn't exist")
		return
	# block[1] - server on which it is stored
    # block[0] - id of the block
	for block in file_table:
		for m in [name_server.get_storage_servers()[_] for _ in block[1]]:
			data = read_from_storage(block[0], m)
			if data:
				sys.stdout.write(data)
				break
			else:
				LOG.info("No blocks found. Possibly a corrupt file")

# handle if it is exists
def write_file(name_server, src, dest):
	size = os.path.getsize(src)
	blocks = name_server.write(dest, size)
	with open(src) as f:
		for b in blocks:
			data = f.read(name_server.get_block_size())
			block_uuid=b[0]
			storage_servers = [name_server.get_storage_servers()[_] for _ in b[1]]
			send_to_storage(block_uuid,data,storage_servers)
	return 0

def delete_from_storage_server(block_uuid, storage_server):
    host,port=storage_server

    con=rpyc.connect(host,port=port, config = {"allow_public_attrs" : True})
    storage_server = con.root
    storage_server.delete_file(block_uuid, storage_server)

def delete_file(naming_server, fname):
	file_table = naming_server.read(fname)

	storage_servers = naming_server.get_storage_servers()
	if not file_table:
		LOG.info("404: File is empty")
		return

	for block in file_table:
		print([storage_servers[_] for _ in block[1]])
		for _ in [storage_servers[_] for _ in block[1]]:
			delete_from_storage_server(block[0], storage_servers)


# def delete_file(name_server, path):
# 	file_table = name_server.read(path)
# 	if not file_table:
# 		LOG.info("404: file not found")
# 		return
# 	storage_servers = name_server.get_storage_servers()
# 	print(storage_servers)
# 	for block in file_table:
# 		for m in [storage_servers[_] for _ in block[1]]:
# 			print(m)
# 			host,port = m
# 			con=rpyc.connect(host,port=port, config = {"allow_public_attrs" : True})
# 			storage= con.root
# 			print(block[0])
# 			storage.delete_file(block[0], storage_servers)
# 	return

# TODO check the implementation in the 
def info_file(name_server, path):
	info = name_server.get_info(path)
	sys.stdout.write(info)
	return
	
# def copy_file(name_server, src, dest):
# 	file = read_file(name_server, src)
# 	write_file(name_server, dest, file)
# 	return 

def move_file(name_server, src, dest):
	#copy_file(name_server, src, dest)
	delete_file(name_server, src)
	return 0

def open_dir(name_server, path):
	# change directory on all storage server
	storage_servers = name_server.get_storage_servers()
	for storage in storage_servers:
		host,port = storage[1]
		con=con.root(host,port=port, config = {"allow_public_attrs" : True})
		storage = con.root
		return storage.open_dir(path) 


def read_dir(name_server, path):
	file_table = name_server.read(path)
	files = file_table.keys()
	sys.stdout.write(files)
	return 0

def make_dir(name_server, path):
	name_server.make_dir(path)
	return 0

def delete_dir(name_server, path):
	storage_servers = name_server.get_storage_servers()
	for storage in storage_servers:
		host,port = storage[1]
		con=con.root(host,port=port, config = {"allow_public_attrs" : True})
		storage = con.root
		return storage.delete_dir(path) 

def list_entries(name_server, path):
	entries = name_server.list_dir(path)
	print('\n'.join(entries))


def main(args):
	con=rpyc.connect("127.0.0.1", port=2000, config = {"allow_public_attrs" : True})
	naming_server=con.root

	menu = sys.argv[1]

	if menu == 'init':
		initialize(naming_server)
	elif menu == 'create':
		create_file(naming_server, sys.argv[2])
	elif menu == 'read':
		read_file(naming_server, sys.argv[2])
	elif menu == 'write':
		write_file(naming_server, sys.argv[2], sys.argv[3])
	elif menu == 'delete':
		delete_file(naming_server, sys.argv[2])
	elif menu == 'info':
		info_file(naming_server, sys.argv[2])
	elif menu == 'copy':
		#copy_file(naming_server, sys.argv[2], sys.argv[3])
		print("Implementation undergoing construction")
	elif menu == 'move':
		move_file(naming_server, sys.argv[2], sys.argv[3])
	elif menu == 'opndir':
		open_dir(naming_server, sys.argv[2])
	elif menu == 'rddir':
		read_dir(naming_server, sys.argv[2])
	elif menu == 'mkdir':
		make_dir(naming_server, sys.argv[2])
	elif menu == 'dltdir':
		delete_dir(naming_server, sys.argv[2])
	elif menu == 'listall':
		list_entries(naming_server, sys.argv[2])
	else:
		print("Possible commands")
		print("-------------------------")
		print("init")
		print("create <file_name>")
		print("read <file_name>")
		print("write <src_file> <dest_file>")
		print("delete <file_name>")
		print("info <file_name>")
		print("copy src_file dest_file")
		print("move src dest")
		print("opndir directory/")
		print("rddir directory/")
		print("mkdir directory/")
		print("dltdir directory/")
		LOG.error("incorrect command")


if __name__ == "__main__":
    main(sys.argv[1:])