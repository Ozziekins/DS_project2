import rpyc
import uuid
import threading 
import math
import random
import sys
import os
import pprint
from Directory import Directory
from datetime import date


from rpyc.utils.server import ThreadedServer

BLOCK_SIZE = 128
REPLICATION_FACTOR = 2
STORAGESERVER = {"1":("127.0.0.1", 5000), "2":("127.0.0.1",6000)}

class MasterService(rpyc.Service):
    file_tree = Directory('','')
    storage_servers = STORAGESERVER
    block_size = BLOCK_SIZE
    replication_factor = REPLICATION_FACTOR

    def exposed_read(self,fname):
        file = self.__class__.file_tree.get_file(fname)
        return file.get_mapping()

    def exposed_create_file(self, fname):
        self.__class__.file_tree.create_file(fname)

    def exposed_get_info(self, fname):
        file = self.__class__.file_tree.get_file(fname)
        return file.get_info()


    def exposed_make_dir(self,dir_name):
        self.__class__.file_tree.create_directory(dir_name)
    
    def exposed_open_dir(self, path):
        current_dir = self.__class__.file_tree.open_directory(path)
        return current_dir

    def exposed_write(self,dest,size):
        # if self.exists(dest):
        #     print(f'File {dest} already exist')
        #     return
        self.__class__.file_tree.create_file(dest)
        num_blocks = self.calc_num_blocks(size)
        blocks = self.alloc_blocks(dest,num_blocks)

        return blocks

    def exposed_get_block_size(self):
      return self.__class__.block_size

    def exposed_get_storage_servers(self):
      return self.__class__.storage_servers

    def calc_num_blocks(self,size):
      return int(math.ceil(float(size)/self.__class__.block_size))

    def exists(self,file):
        if file in self.__class__.file_tree.get_files().keys():
            return True
        return False

    def alloc_blocks(self,dest,numblks):
        blocks = []
        size = numblks * self.__class__.block_size
        file = self.__class__.file_tree.get_file(dest)
        file.set_size(size)
        file.set_last_modified(date.today())
        for _ in range(0,numblks):
            block_id = uuid.uuid1()
            nodes_ids = random.sample(self.__class__.storage_servers.keys(),self.__class__.replication_factor)
            blocks.append((block_id,nodes_ids))

            file.add_mapping((block_id,nodes_ids))
        print(blocks)
        return blocks


if __name__ == "__main__":
  t = ThreadedServer(MasterService, port = 2000, protocol_config = {"allow_public_attrs" : True})
  t.start()