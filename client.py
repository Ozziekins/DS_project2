import rpyc
import sys
import os
import logging

logging.basicConfig(level=logging.DEBUG)
LOG = logging.getLogger(__name__)

def send_to_storage_server(block_uuid,data,storage_servers):
    LOG.info("sending: " + str(block_uuid) + str(storage_servers))
    storage_server=storage_servers[0]
    storage_servers=storage_servers[1:]
    host,port=storage_server

    con=rpyc.connect(host,port=port, config = {"allow_public_attrs" : True})
    storage_server = con.root
    storage_server.write_file(block_uuid,data,storage_servers)


def read_from_storage_server(block_uuid,storage_server):
    host,port = storage_server
    con=rpyc.connect(host,port=port, config = {"allow_public_attrs" : True})
    storage_server = con.root
    return storage_server.read_file(block_uuid)

def initialize_storage_server(storage_server):
    host,port = storage_server
    con=rpyc.connect(host,port=port, config = {"allow_public_attrs" : True})
    storage_server = con.root
    return storage_server.initialize_storage()

def get(naming_server,fname):
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

def put(naming_server,source,dest):
    size = os.path.getsize(source)
    blocks = naming_server.write(dest,size)
    with open(source) as f:
        for b in blocks:
            data = f.read(naming_server.get_block_size())
            block_uuid=b[0]
            storage_servers = [naming_server.get_storage_servers()[_] for _ in b[1]]
            send_to_storage_server(block_uuid,data,storage_servers)

def initialize(naming_server):
    storage_servers = naming_server.get_storage_servers()
    for i in range(len(storage_servers)):
        m = storage_servers['{}'.format(i+1)]
        initialize_storage_server(m)


def main(args):
    con=rpyc.connect("127.0.0.1",port=2000, config = {"allow_public_attrs" : True})
    naming_server=con.root
    
    if args[0] == "read":
        get(naming_server, args[1])
    elif args[0] == "write":
        put(naming_server, args[1], args[2])
    elif args[0] == "init":
        initialize(naming_server)
    else:
        LOG.error("try 'put srcFile destFile OR get file'")


if __name__ == "__main__":
    main(sys.argv[1:])
