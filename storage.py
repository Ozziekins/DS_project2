import rpyc
import uuid
import os
import argparse
from rpyc.utils.server import ThreadedServer
import shutil

class StorageService(rpyc.Service):

    def exposed_initialize_storage(self):
        shutil.rmtree(DATA_DIR)


    def exposed_write_file(self,block_id,data,storage_servers):
        block_addr=DATA_DIR+str(block_id)
        with open(block_addr,'w') as f:
            f.write(data)
        if len(storage_servers)>0:
            self.replicate(block_id, data, storage_servers)
        print("Directory Content:")
        print('\n'.join(os.listdir(DATA_DIR)))


    def exposed_read_file(self,block_id):
        block_addr=DATA_DIR+str(block_id)
        if not os.path.isfile(block_addr):
            return None
        with open(block_addr) as f:
            return f.read()   
    


    def replicate(self,block_id,data,minions):
        print(f'{PORT}: forwaring to:')
        print(block_id, minions)
        minion=minions[0]
        minions=minions[1:]
        host,port=minion

        con=rpyc.connect(host,port=port, config = {"allow_public_attrs" : True})
        minion = con.root
        minion.write_file(block_id,data,minions)

    def delete_block(self,uuid):
        pass

if __name__ == "__main__":
    argument = argparse.ArgumentParser()
    argument.add_argument("-p", "--port", required=True, help="port number needed")
    argument.add_argument("-dir", "--directory", required=True, help="operating directory")
    args = vars(argument.parse_args())
    PORT = int(args['port'])
    DATA_DIR = args['directory']
    if not os.path.isdir(DATA_DIR): os.mkdir(DATA_DIR)

    t = ThreadedServer(StorageService, port = PORT,)
    t.start()