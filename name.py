import rpyc
import uuid
import threading 
import math
import random
import signal
import pickle
import sys
import os
import pprint

from rpyc.utils.server import ThreadedServer

BLOCK_SIZE = 128
REPLICATION_FACTOR = 2
MINIONS = {"1":("127.0.0.1", 5000), "2":("127.0.0.1",6000)}

class MasterService(rpyc.Service):
    file_table = {}
    block_mapping = {}
    minions = MINIONS

    block_size = BLOCK_SIZE
    replication_factor = REPLICATION_FACTOR

    def exposed_read(self,fname):
      mapping = self.__class__.file_table[fname]
      return mapping

    def exposed_write(self,dest,size):
      if self.exists(dest):
        pass # ignoring for now, will delete it later

      self.__class__.file_table[dest]=[]

      num_blocks = self.calc_num_blocks(size)
      blocks = self.alloc_blocks(dest,num_blocks)
      return blocks

    def exposed_get_file_table_entry(self,fname):
      if fname in self.__class__.file_table:
        return self.__class__.file_table[fname]
      else:
        return None

    def exposed_get_block_size(self):
      return self.__class__.block_size

    def exposed_get_minions(self):
      return self.__class__.minions

    def calc_num_blocks(self,size):
      return int(math.ceil(float(size)/self.__class__.block_size))

    def exists(self,file):
      return file in self.__class__.file_table

    def alloc_blocks(self,dest,num):
        blocks = []
        for _ in range(0,num):
            block_uuid = uuid.uuid1()
            nodes_ids = random.sample(self.__class__.minions.keys(),self.__class__.replication_factor)
            blocks.append((block_uuid,nodes_ids))

            self.__class__.file_table[dest].append((block_uuid,nodes_ids))
        pprint.pprint(self.__class__.file_table)
        return blocks


if __name__ == "__main__":
  t = ThreadedServer(MasterService, port = 2000, protocol_config = {"allow_public_attrs" : True})
  t.start()