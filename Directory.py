from datetime import date
from File import File

class Directory:
    def __init__(self, name, path):
        self.name = name
        self.size = 0
        self.date_created = date.today()
        self.last_modified = self.date_created
        self.files = {}
        self.directories = {}
        self.location = path + '/'

    def is_directory(self):
        return True

    def open_directory(self, path):
        for x in self.directories.values():
            if path.find(x.get_location()) >= 0:
                if path == x.get_location():
                    return x
                else:
                    if len(x.get_directories()) != 0:
                        x.open_directory(path)
        self.create_directory(path)
        self.open_directory(path)

    def create_file(self, file_name, size):
        self.add_size(size)
        self.files[file_name] = File(file_name, '', size)

    def create_directory(self, directory_name):
        self.directories[directory_name] = Directory(directory_name, self.location + directory_name)

    def get_directory(self, directory_name):
        return self.directories[directory_name]
    
    def get_directories(self):
        return self.directories
    
    def get_size(self):
        return self.size

    def get_location(self):
        return self.location

    def get_file(self, fname):
        return self.files[fname]
    
    def get_files(self):
        return self.files

    def delete_file(self, file_name):
        if self.exist_file(file_name):
            del self.files[file_name]

    def delete_files(self):
        file_names = list(self.get_files().keys())
        for file in file_names:
            self.delete_file(file)
    
    def delete_directory(self, dir_name):
        if self.exist_directory(dir_name):
            del self.directories[dir_name]

    def delete_directories(self):
        dir_names = list(self.get_directories().keys())
        for dir in dir_names:
            self.delete_directory(dir)
    
    def get_name(self):
        return self.name

    def add_size(self, amount):
        self.size = self.size + amount
    
    def exist_file(self, file_name):
        if self.files.get(file_name) != None:
            return True
        return False

    def exist_directory(self, dir_name):
        if self.directories.get(dir_name) != None:
            return True
        return False

    def rename_file(self, original_name, new_name):
        self.files[new_name] = self.files[original_name]
        self.delete_file(original_name)

    def is_empty(self):
        if self.files and self.directories:
            return False
        return True
    
    def list_dir(self, path):
      ls = list()
      dirs = self.get_directories().keys()
      files = self.get_files().keys()
      ls.extend([*dirs])
      ls.extend([*files])
      return ls
    
    def init(self):
        self.delete_directories()
        self.delete_files()
        self.size = 0
        self.last_modified = date.today()

# update size, last_modified on delete and update operations