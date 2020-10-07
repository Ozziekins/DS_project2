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
	#TODO check the name
	storage = con.root
	return storage.read_file(block_uuid)

def initialize_storage_server(storage_server):
    host,port = storage_server
    con=rpyc.connect(host,port=port, config = {"allow_public_attrs" : True})
    storage_server = con.root
    return storage_server.initialize_storage()
 
 # delete from name sever 
def initialize(name_server):
    storage_servers = name_server.get_storage_servers()
    for i in range(len(storage_servers)):
        m = storage_servers['{}'.format(i+1)]
        initialize_storage_server(m)
    name_server.initialize()

# TODO check the argument
def create_file(name_server, path):
	name_server.create_file(path, 0)
	return 0


def read_file(name_server, path):
	#TODO check the name of name_server
	file_table = name_server.read(path)
	if not file_table:
		LOG.info("404: file not found")
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

def delete_from_storage_server(block_uuid, storage_servers):
    LOG.info("deleting: " + str(block_uuid) + str(storage_servers))
    storage=storage_servers[0]
    storage_servers=storage_servers[1:]
    host,port=storage

    con=rpyc.connect(host,port=port, config = {"allow_public_attrs" : True})
    storage = con.root
    storage.delete_file(block_uuid, storage_servers)

def delete_file(name_server, fname):
    file_table = name_server.read(fname)
    
    if not file_table:
        LOG.info("404: file not found")
        return

    for block in file_table:
        storage_servers = [name_server.get_storage_servers()[_] for _ in block[1]]
        delete_from_storage_server(block[0], storage_servers)

def info_file(name_server, path):
	info = name_server.get_info(path)
	sys.stdout.write(info)
	return
	
def copy_file(name_server, src, dest):
	#TODO check the name of name_server
	file_table = name_server.read(src)
	file_data = ""
	if not file_table:
		LOG.info("404: file not found")
		return
	for block in file_table:
		for m in [name_server.get_storage_servers()[_] for _ in block[1]]:
			data = read_from_storage(block[0], m)
			if data:
				file_data = file_data + data
				break
			else:
				LOG.info("No blocks found. Possibly a corrupt file")
	f = open("temp", "w+")
	f.write(file_data)
	f.close()
	write_file(name_server, 'temp', dest)
	os.remove("temp") 
	return 

def move_file(name_server, src, dest):
	copy_file(name_server, src, dest)
	delete_file(name_server, src)
	return 0

def open_dir(name_server, path):
	# change directory on all storage server
	name_server.open_dir(path)
	storage_servers = name_server.get_storage_servers()
	for storage in storage_servers:
		host,port = storage[1]
		con=con.root(host,port=port, config = {"allow_public_attrs" : True})
		storage = con.root
		return storage.open_dir(path) 


def read_dir(name_server, path):
	files = name_server.list_dir(path)
	for f in files:
		sys.stdout.write(f)
		sys.stdout.write('\n')
	return 0

def make_dir(name_server, path):
	name_server.make_dir(path)
	storage_servers = name_server.get_storage_servers()
	store = [*storage_servers.keys()]
	for i in store:
		storage = storage_servers[i]
		host,port = storage
		con=rpyc.connect(host,port=port, config = {"allow_public_attrs" : True})
		storage = con.root
		storage.make_dir(path)
	return 0

def delete_dir(name_server, path):
	print("Are you sure, buddy y/N?")
	answer = input()
	if answer == "" or answer == "N" or answer == "n":
		return
	elif answer == "y" or answer == "Y":
		storage_servers = name_server.get_storage_servers()
		store = [*storage_servers.keys()]
		for i in store:
			storage = storage_servers[i]
			host,port = storage
			con=rpyc.connect(host,port=port, config = {"allow_public_attrs" : True})
			storage = con.root
			storage.delete_dir(path)
		name_server.delete_dir(path)
	else:
		print("Character not recongnised")

def main(args):
	con=rpyc.connect("127.0.0.1", port=2000, config = {"allow_public_attrs" : True})
	name_server=con.root

	menu = sys.argv[1]

	if menu == 'init':
		initialize(name_server)
	elif menu == 'create':
		create_file(name_server, sys.argv[2])
	elif menu == 'read':
		read_file(name_server, sys.argv[2])
	elif menu == 'write':
		write_file(name_server, sys.argv[2], sys.argv[3])
	elif menu == 'delete':
		delete_file(name_server, sys.argv[2])
	elif menu == 'info':
		info_file(name_server, sys.argv[2])
	elif menu == 'copy':
		copy_file(name_server, sys.argv[2], sys.argv[3])
		# print("Implementation undergoing construction")
	elif menu == 'move':
		move_file(name_server, sys.argv[2], sys.argv[3])
	elif menu == 'cd':
		open_dir(name_server, sys.argv[2])
	elif menu == 'ls':
		read_dir(name_server, sys.argv[2])
	elif menu == 'mkdir':
		make_dir(name_server, sys.argv[2])
	elif menu == 'dltdir':
		delete_dir(name_server, sys.argv[2])
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
		print("cd directory/")
		print("ls directory/")
		print("mkdir directory/")
		print("dltdir directory/")
		LOG.error("incorrect command")


if __name__ == "__main__":
    main(sys.argv[1:])