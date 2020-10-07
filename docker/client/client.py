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
    size = name_server.initialize()
    print(f'{size} bytes available\n')

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
    name_server.delete_file(fname)
    if not file_table:
        LOG.info("404: file not found")
        return

    for block in file_table:
        storage_servers = [name_server.get_storage_servers()[_] for _ in block[1]]
        LOG.info(storage_servers)
        delete_from_storage_server(block[0], storage_servers)
    

def info_file(name_server, path):
	info = name_server.get_info(path)
	sys.stdout.write(info)
	return
	
def copy_file(naming_server, src):
    # check if file exists, if no give error message
    if not naming_server.exists(src):
        LOG.info("404: file not found")
        return

    base, ext = src.split(".")

    contents = naming_server.list_dir()

    copies = base + "_copy"
    num = []

    # check if any copies already exist
    for c in contents:
        # get all numbers of the copies
        if copies in c:
            i = int(''.join(x for x in c if x.isdigit()))
            num.append(i)


    # and there is no copy, create the first copy
    if len(num) == 0:
        base_copy = base + f'_copy{1}'
        dest = base_copy + "." + ext
    else:
        # if there is a copy get the latest copy and increase the number by one
        j = max(num) + 1
        base_copy = copies + f'{j}'
        dest = base_copy + "." + ext

    file_table = naming_server.read(src)
    file_data = ""
    if not file_table:
        LOG.info("404: file not found")
        return
    for block in file_table:
        for m in [naming_server.get_storage_servers()[_] for _ in block[1]]:
            data = read_from_storage(block[0], m)
            if data:
                file_data = file_data + data
                break
            else:
                LOG.info("No blocks found. Possibly a corrupt file")
    f = open("temp", "w+")
    f.write(file_data)
    f.close()
    write_file(naming_server, 'temp', dest)
    os.remove("temp") 
    return 

def move_file(name_server, src, dest):
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
    delete_file(name_server, src)
    open_dir(name_server, dest)
    write_file(name_server, 'temp', src)
    open_root(name_server)
    os.remove("temp") 
    return 0

def open_dir(name_server, path):
	# change directory on all storage server
    if not name_server.directory_exists(path):
        print("Directory does not exist")
        return
    name_server.open_dir(path)
    storage_servers = name_server.get_storage_servers()
    store = storage_servers.keys()
    for i in store:
        storage = storage_servers[i]
        host,port = storage
        con=rpyc.connect(host,port=port, config = {"allow_public_attrs" : True})
        storage = con.root
        storage.open_dir(path)

def open_root(name_server):
    name_server.open_root()
    storage_servers = name_server.get_storage_servers()
    store = storage_servers.keys()
    for i in store:
        storage = storage_servers[i]
        host,port = storage
        con=rpyc.connect(host,port=port, config = {"allow_public_attrs" : True})
        storage = con.root
        storage.open_root()

def read_dir(name_server):
	files = name_server.list_dir()
	for f in files:
		sys.stdout.write(f)
		sys.stdout.write('\n')
	return 0

def make_dir(name_server, path):
    name_server.make_dir(path)
    storage_servers = name_server.get_storage_servers()
    store = storage_servers.keys()
    for i in store:
        storage = storage_servers[i]
        host,port = storage
        con=rpyc.connect(host,port=port, config = {"allow_public_attrs" : True})
        storage = con.root
        storage.make_dir(path)
    return 0

def delete_dir(name_server, path):
    if not name_server.is_empty(path):
        print("Are you sure y/N?")
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
    else:
        storage_servers = name_server.get_storage_servers()
        store = [*storage_servers.keys()]
        for i in store:
            storage = storage_servers[i]
            host,port = storage
            con=rpyc.connect(host,port=port, config = {"allow_public_attrs" : True})
            storage = con.root
            storage.delete_dir(path)
        name_server.delete_dir(path)

def main(args):
    con=rpyc.connect("127.0.0.1", port=2000, config = {"allow_public_attrs" : True})
    naming_server=con.root

    while True:
        print()
        print("Enter command: ")
        cmd = input("$: ") 
        words = cmd.split()
        menu = words[0]

        if menu == 'init':
            initialize(naming_server)
        elif menu == 'create':
            create_file(naming_server, words[1])
        elif menu == 'read':
            read_file(naming_server, words[1])
        elif menu == 'write':
            write_file(naming_server, words[1], words[2])
        elif menu == 'delete':
            delete_file(naming_server, words[1])
        elif menu == 'info':
            info_file(naming_server, words[1])
        elif menu == 'copy':
            copy_file(naming_server, words[1])
        elif menu == 'move':
            move_file(naming_server, words[1], words[2])
        elif menu == 'cd':
            open_dir(naming_server, words[1])
        elif menu == 'root':
            open_root(naming_server)
        elif menu == 'ls':
            read_dir(naming_server)
        elif menu == 'mkdir':
            make_dir(naming_server, words[1])
        elif menu == 'dltdir':
            delete_dir(naming_server, words[1])
        elif menu == 'exit':
            break
        else:
            print("Possible commands")
            print("-------------------------")
            print("init")
            print("create <file_name>")
            print("read <file_name>")
            print("write <src_file> <dest_file>")
            print("delete <file_name>")
            print("info <file_name>")
            print("copy src_file")
            print("move src dest")
            print("root")
            print("cd directory/")
            print("ls directory/")
            print("mkdir directory/")
            print("dltdir directory/")
            LOG.error("incorrect command")

if __name__ == "__main__":
    main(sys.argv[1:])