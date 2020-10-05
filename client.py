import rpyc
import sys
import os
import logging
import argparse

logging.basicConfig(level=logging.DEBUG)
LOG = logging.getLogger(__name__)

def initialize_storage_server(storage_server):
    host,port = storage_server
    con=rpyc.connect(host,port=port, config = {"allow_public_attrs" : True})
    storage_server = con.root
    return storage_server.initialize_storage()

def initialize(naming_server):
    storage_servers = naming_server.get_storage_servers()
    for i in range(len(storage_servers)):
        m = storage_servers['{}'.format(i+1)]
        initialize_storage_server(m)

def read_from_storage_server(block_uuid, storage_server):
    host,port = storage_server
    con=rpyc.connect(host,port=port, config = {"allow_public_attrs" : True})
    storage_server = con.root
    return storage_server.read_file(block_uuid)

def read_file(naming_server, fname):
    file_table = naming_server.get_file_table_entry(fname)
    if not file_table:
        LOG.info("404: file not found")
        return

    for block in file_table:
        for m in [naming_server.get_storage_servers()[_] for _ in block[1]]:
            data = read_from_storage_server(block[0],m)
            if data:
                sys.stdout.write(data)
                break
            else:
                LOG.info("No blocks found. Possibly a corrupt file")

def write_to_storage_server(block_uuid, data, storage_servers):
    LOG.info("sending: " + str(block_uuid) + str(storage_servers))
    storage_server=storage_servers[0]
    storage_servers=storage_servers[1:]
    host,port=storage_server

    con=rpyc.connect(host, port=port, config={"allow_public_attrs" : True})
    storage_server = con.root
    storage_server.write_file(block_uuid, data, storage_servers)

def write_file(naming_server, source, dest):
    size = os.path.getsize(source)
    blocks = naming_server.write(dest,size)
    with open(source) as f:
        for b in blocks:
            data = f.read(naming_server.get_block_size())
            block_uuid=b[0]
            storage_servers = [naming_server.get_storage_servers()[_] for _ in b[1]]
            write_to_storage_server(block_uuid, data, storage_servers)

def delete_from_storage_server(block_uuid, storage_server):
    host,port=storage_server

    con=rpyc.connect(host,port=port, config = {"allow_public_attrs" : True})
    storage_server = con.root
    storage_server.delete_file(block_uuid, storage_server)

def delete_file(naming_server, fname):
    file_table = naming_server.get_file_table_entry(fname)
    if not file_table:
        LOG.info("404: file not found")
        return

    for block in file_table:
        for m in [naming_server.get_storage_servers()[_] for _ in block[1]]:
            delete_from_storage_server(block[0], m)

def main(args):
    con=rpyc.connect("127.0.0.1",port=2000, config = {"allow_public_attrs" : True})
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
        copy_file(naming_server, sys.argv[2], sys.argv[3])
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
