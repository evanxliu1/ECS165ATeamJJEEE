class Page:



    def __init__(self):

        self.num_records = 0

        self.data = bytearray(4096)



    def has_capacity(self):

        return self.num_records < 512



    def write(self, value):

        if not self.has_capacity():

            return False

        start_index = self.num_records * 8

    

        if value is None:

            value = 0 

            

        bytes_value = int(value).to_bytes(8, byteorder='little')


        self.data[start_index : start_index + 8] = bytes_value

        

        self.num_records += 1

        return True



   

    def read(self, record_index):

        if record_index >= self.num_records:

            return None 



        start_index = record_index * 8

        bytes_value = self.data[start_index : start_index + 8]

        

        return int.from_bytes(bytes_value, byteorder='little')
