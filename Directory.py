from uuid import uuid4
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
        for x in self.directories:
            if path.find(x.get_location) >= 0:
                if path == x.get_location:
                    return x
                else:
                    if len(x.get_directories) != 0:
                        x.open_directory(path)
                    return None
            

    def create_file(self, file_name):
        self.files[file_name] = File(file_name, '')

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
            todel = self.files[file_name]
            todel.delete()
            del self.files[file_name]

    def delete_directory(self):
        del self.files
        del self.directories
    
    def exist_file(self, file_name):
        if self.files.get(file_name) != None:
            return True
        return False

    def rename_file(self, original_name, new_name):
        self.files[new_name] = self.files[original_name]
        self.delete_file(original_name)

    def isEmpty(self):
        if self.files and self.directories:
            return False
        return True

# update size, last_modified on delete and update operations