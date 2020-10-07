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
REPLICATION_FACTOR = 1
STORAGESERVER = {"1":("127.0.0.1", 5000)}
# {"1":("127.0.0.1", 5000), "2":("127.0.0.1",6000), "3":("127.0.0.1",7000)}
MAX_SIZE = 512000

class NameService(rpyc.Service):
    root = Directory('','')
    file_tree = root
    storage_servers = STORAGESERVER
    block_size = BLOCK_SIZE
    replication_factor = REPLICATION_FACTOR
    available_size = MAX_SIZE

    def exposed_open_dir(self, path):
        self.__class__.file_tree = self.__class__.file_tree.open_directory(path)
        
    def exposed_open_root(self):
        self.__class__.file_tree = self.__class__.root

    def exposed_initialize(self):
        self.__class__.file_tree.init()
        return self.__class__.available_size

    def exposed_read(self,fname):
        file = self.__class__.file_tree.get_file(fname)
        return file.get_mapping()

    def exposed_create_file(self, fname, size):
        self.__class__.file_tree.create_file(fname, size)

    def exposed_get_info(self, fname):
        file = self.__class__.file_tree.get_file(fname)
        return file.get_info()

    def exposed_make_dir(self,dir_name):
        self.__class__.file_tree.create_directory(dir_name)
        
    def exposed_list_dir(self):
      directory_items = self.__class__.file_tree.list_dir()
      return directory_items

    def exposed_delete_dir(self, dir_name):
        self.__class__.file_tree.delete_directory(dir_name)

    def exposed_write(self,dest,size):
        self.__class__.available_size = self.__class__.available_size - size
        self.__class__.file_tree.create_file(dest, size)
        num_blocks = self.calc_num_blocks(size)
        blocks = self.alloc_blocks(dest,num_blocks)
        return blocks

    def exposed_delete_file(self, fname):
        self.__class__.file_tree.delete_file(fname)
    
    def exposed_directory_exists(self, dir_name):
        return self.__class__.file_tree.exist_directory(dir_name)

    def exposed_is_empty(self, path):
        directory = self.__class__.file_tree.open_directory(path)
        if directory.is_empty():
            return True
        return False

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
        return blocks

# initialize the entire system 
if __name__ == "__main__":
  t = ThreadedServer(NameService, port = 2000, protocol_config = {"allow_public_attrs" : True})
  t.start()