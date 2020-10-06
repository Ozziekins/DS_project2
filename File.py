from datetime import date


class File:
    def __init__(self, name, parent_location, size):
        self.name = name
        self.location = parent_location + '/' + name
        self.size = size
        self.date_created = date.today()
        self.last_modified = self.date_created
        self.mappings = []

    def get_name(self):
        return self.name

    def delete(self):
        self.size = 0
        del self.mappings

    def is_directory(self):
        return False

    def get_size(self):
        return self.size

    def get_mapping(self):
        return self.mappings
    
    def set_last_modified(self, date):
        self.last_modified = date

    def set_size(self, size):
        self.size = size

    def get_info(self):
        info = (f'name: {self.name}\n'\
                f'location: {self.location}\n'\
                f'size: {self.size} bytes\n'\
                f'date created: {self.date_created}\n'\
                f'last modified: {self.last_modified}\n')
        return info

    def add_mapping(self, block_map):
        self.mappings.append(block_map)

