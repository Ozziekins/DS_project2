import socket                   
import sys
import os, time
from stat import * # ST_SIZE etc
from shutil import copyfile, move

# Create a socket object
# s = socket.socket()
# host = localhost
# port = 123

def read_from_storage(block_uuid,minion):
  host,port = minion
  con=rpyc.connect(host,port=port)
  minion = con.root.Minion()
  return minion.get(block_uuid)


def init():
	return 0

def create_file(path):
	print("Creating the file ... ")
	file = path
	print("File created") 
	return 0


def read_file(name_server, path):
	#TODO check the name of name_server
	file_table = name_server.get_file_table_entry(path)
	if not file_table:
    	LOG.info("404: file not found")
    return

    for block in file_table:
    	# block[1] - server on which it is stored
    	# block[0] - id of the block
    	for m in [name_server.get_minions()[_] for _ in block[1]]

def write_file(path, content):
	print("Writing to file")
	file = path
	info = content
	print("File changed") 
	return 0

def delete_file(path):
	print("Deleting the file")
	file = path
	print("File removed") 
	return 0

def info_file(path):
	print("Information about the file")
	file  = path
	return 0

def copy_file(src, dest):
	print("Copying the file from src to dest ...")
	source_file = src
	destination_file = dest
	print("File copied") 
	return 0

def move_file(src, dest):
	print("Moving the file from src to dest ...")
	source_file = src
	destination_file = dest
	print("File moved") 
	return 0

def open_dir(path):
	print("Changing the directory")
	directory = path
	print("Directory changed") 
	return 0

def read_dir(path):
	print("Listing everything in dir ...")
	directory - path
	return 0

def make_dir(path):
	print("Creating the dir...")
	directory = path
	print("Diirectory created")
	return 0

def delete_dir(path):
	print("Removing th dir ...")
	directory = path
	print("Dir deleted")
	return 0

# con=rpyc.connect("localhost",port=2131)
# name_server=con.root.Master()
menu = sys.argv[1]

if menu == 'init':
	init()
elif menu == 'create_file':
	create_file(sys.argv[2])
elif menu == 'read_file':
	read_file(sys.argv[2])
elif menu == 'write_file':
	write_file(sys.argv[2], sys.argv[3])
elif menu == 'delete_file':
	delete_file(sys.argv[2])
elif menu == 'info_file':
	info_file(sys.argv[2])
elif menu == 'copy_file':
	copy_file(sys.argv[2], sys.argv[3])
elif menu == 'move_file':
	move_file(sys.argv[2], sys.argv[3])
elif menu == 'open_dir':
	open_dir(sys.argv[2])
elif menu == 'read_dir':
	read_dir(sys.argv[2])
elif menu == 'make_dir':
	make_dir(sys.argv[2])
elif menu == 'delete_dir':
	delete_dir(sys.argv[2])
else:
  LOG.error("try 'put srcFile destFile OR get file'")






