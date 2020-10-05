import rpyc
import sys
import os
import logging

logging.basicConfig(level=logging.DEBUG)
LOG = logging.getLogger(__name__)

def initialize_storage_server(storage_server):
    host,port = storage_server
    con=rpyc.connect(host,port=port, config = {"allow_public_attrs" : True})
    storage_server = con.root
    return storage_server.initialize_storage()
 
 # delete from name sever 
def initialize(naming_server):
    storage_servers = naming_server.get_storage_servers()
    for i in range(len(storage_servers)):
        m = storage_servers['{}'.format(i+1)]
        initialize_storage_server(m)

def create_file(naming_server, path):
    naming_server.create_file(path)
    return 0

def read_from_storage(block_uuid,storage):
    host,port = storage
    con=rpyc.connect(host,port=port, config = {"allow_public_attrs" : True})
    
    storage = con.root
    return storage.read_file(block_uuid)

def read_file(naming_server, path):
    file_table = naming_server.read(path)
    if not file_table:
        LOG.info("404: file not found")
        return
    # block[1] - server on which it is stored
    # block[0] - id of the block
    for block in file_table:
        for m in [naming_server.get_storage_servers()[_] for _ in block[1]]:
            data = read_from_storage(block[0], m)
            if data:
                sys.stdout.write(data)
                break
            else:
                LOG.info("No blocks found. Possibly a corrupt file")

def write_to_storage(block_uuid,data,storage_servers):
    LOG.info("sending: " + str(block_uuid) + str(storage_servers))
    storage=storage_servers[0]
    storage_servers=storage_servers[1:]
    host,port=storage

    con=rpyc.connect(host,port=port, config = {"allow_public_attrs" : True})
    storage = con.root
    storage.write_file(block_uuid,data,storage_servers)

def write_file(naming_server, src, dest):
    size = os.path.getsize(src)
    print(size)
    blocks = naming_server.write(dest, size)
    with open(src) as f:
        for b in blocks:
            data = f.read(naming_server.get_block_size())
            block_uuid=b[0]
            storage_servers = [naming_server.get_storage_servers()[_] for _ in b[1]]
            write_to_storage(block_uuid,data,storage_servers)
    return 0

def delete_from_storage_server(block_uuid, storage_servers):
    LOG.info("deleting: " + str(block_uuid) + str(storage_servers))
    storage=storage_servers[0]
    storage_servers=storage_servers[1:]
    host,port=storage

    con=rpyc.connect(host,port=port, config = {"allow_public_attrs" : True})
    storage = con.root
    storage.delete_file(block_uuid, storage_servers)

def delete_file(naming_server, fname):
    file_table = naming_server.read(fname)
    
    if not file_table:
        LOG.info("404: file not found")
        return

    for block in file_table:
        storage_servers = [naming_server.get_storage_servers()[_] for _ in block[1]]
        delete_from_storage_server(block[0], storage_servers)

# TODO check the implementation in the 
def info_file(naming_server, path):
    info = naming_server.get_info(path)
    sys.stdout.write(info)
    return
    
def copy_file(naming_server, src, dest):
    file = read_file(naming_server, src)
    write_file(naming_server, dest, file)
    return 

def move_file(naming_server, src, dest):
    copy_file(naming_server, src, dest)
    delete_file(naming_server, src)
    return 0

def open_dir(naming_server, path):
    naming_server.make_dir(path)
    storage_servers = naming_server.get_storage_servers()
    store = [*storage_servers.keys()]

    for i in store:
        storage = storage_servers[i]
        host,port = storage
        con=rpyc.connect(host,port=port, config = {"allow_public_attrs" : True})
        storage = con.root
        storage.open_dir(path)


def read_dir(naming_server, path):
    content = naming_server.list_dir(path)
    for c in content:
        print(c)
    return

def make_dir(naming_server, path):
    naming_server.make_dir(path)
    storage_servers = naming_server.get_storage_servers()
    store = [*storage_servers.keys()]

    for i in store:
        storage = storage_servers[i]
        host,port = storage
        con=rpyc.connect(host,port=port, config = {"allow_public_attrs" : True})
        storage = con.root
        storage.make_dir(path)

def delete_dir(path):
    storage_servers = naming_server.get_storage_servers()
    for storage in storage_servers:
        host,port = storage[1]
        con=con.root(host,port=port, config = {"allow_public_attrs" : True})
        storage = con.root
        return storage.delete_dir(path) 

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
            copy_file(naming_server, words[1], words[2])
        elif menu == 'move':
            move_file(naming_server, words[1], words[2])
        elif menu == 'opndir':
            open_dir(naming_server, words[1])
        elif menu == 'rddir':
            read_dir(naming_server, words[1])
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
            print("copy src_file dest_file")
            print("move src dest")
            print("opndir directory/")
            print("rddir directory/")
            print("mkdir directory/")
            print("dltdir directory/")
            LOG.error("incorrect command")

if __name__ == "__main__":
    main(sys.argv[1:])





