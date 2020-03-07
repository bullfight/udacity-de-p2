import os
import glob
import csv

class Extract:
    def __init__(self, filepath):
        self.filepath = filepath

    def save(self, filename):
        files = self.read_files()
        data  = self.extract(files)
        self.export(data, filename)

    def read_files(self):
        all_files = []
        for root, dirs, files in os.walk(self.filepath):
            files = glob.glob(os.path.join(root,'*.csv'))
            for f in files :
                all_files.append(os.path.abspath(f))

        return all_files

    def extract(self, file_path_list):
        # initiating an empty list of rows that will be generated from each file
        full_data_rows_list = []

        # for every filepath in the file path list
        for f in file_path_list:

            # reading csv file
            with open(f, 'r', encoding = 'utf8', newline='') as csvfile:
                # ceeating a csv reader object
                csvreader = csv.reader(csvfile)
                next(csvreader)

                # extracting each data row one by one and append it
                for line in csvreader:
                    #print(line)
                    full_data_rows_list.append(line)

        return full_data_rows_list

    def export(self, full_data_rows_list, filename):
        # creating a smaller event data csv file called event_datafile_full csv that will be used to insert data into the
        # Apache Cassandra tables
        csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

        with open(filename, 'w', encoding = 'utf8', newline='') as f:
            writer = csv.writer(f, dialect='myDialect')
            writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',\
                    'level','location','sessionId','song','userId'])
            for row in full_data_rows_list:
                if (row[0] == ''):
                    continue
                writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))
