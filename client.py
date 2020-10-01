import socket                   
import sys
import os, time
from stat import * # ST_SIZE etc
from shutil import copyfile, move

# Create a socket object
# s = socket.socket()
# host = localhost
# port = 123

def init():
	return 0

def create_file():
	print("Creating the file ... ")
	print("Please enter file name ")
	name = input()
	print("Please enter the format of file ")
	format_name = input()
	file = open(str(name) + "." + str(format_name), "w+")
	file.close()
	print("File created") 
	return 0

def read_file():
	print("Please enter file name ")
	name = input()
	print("Please enter the format of file ")
	format_name = input()
	file = open(str(name) + "." + str(format_name), "r")
	print("Content of the file:")
	content = file.read()
	print(content)
	return 0

def write_file():
	print("Writing to file")
	print("Please enter file name ")
	name = input()
	print("Please enter the format of file ")
	format_name = input()
	print("Please enter what to write")
	content = input()
	file = open(str(name) + "." + str(format_name), "w+")
	file.write(content)
	file.close()
	print("File changed") 
	return 0

def delete_file():
	print("Deleting the file")
	print("Please enter path to the file ")
	path= input()
	os.remove(path)
	print("File removed") 
	return 0

def info_file():
	print("Information about the file")
	print("Please enter file name ")
	name = input()
	print("Please enter the format of file ")
	format_name = input()
	info = os.stat(str(name) + "." + str(format_name))
	print ("file size:", info[ST_SIZE])
	print ("file modified:", time.asctime(time.localtime(info[ST_MTIME])))
	return 0

def copy_file():
	print("Copying the file from src to dest ...")
	print("Please enter the source of the file with format")
	src = input()
	print("Please enter the destination of the file with format ")
	dest = input()
	copyfile(src, dest)
	print("File copied") 
	return 0

def move_file():
	print("Moving the file from src to dest ...")
	print("Please enter the source of the file with format")
	src = input()
	print("Please enter the destination of the file with format ")
	dest = input()
	move(src, dest)
	print("File moved") 
	return 0

def open_dir():
	print("Changing the directory")
	print("Please enter the path ")
	path  = input()
	os.chdir(path)
	print("Directory changed") 
	return 0

def read_dir():
	print("Listing everything in dir ...")
	print("Please enter the path of the dir")
	path = input()
	os.listdir(path)
	return 0

def make_dir():
	print("Creating the dir...")
	print("Please enter the path")
	path = input()
	os.mkdir( path)
	print("Diirectory created")
	return 0

def delete_dir():
	print("Removing th dir ...")
	print("Please enter the path")
	path = input()
	os.rmdir(path)
	print("Dir deleted")
	return 0

make_dir()
delete_dir()