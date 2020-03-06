# Import Python packages
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv


def load():
    file = 'event_datafile_new.csv'

    with open(file, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader) # skip header
        for line in csvreader:
    ## TO-DO: Assign the INSERT statements into the `query` variable
            query = "<ENTER INSERT STATEMENT HERE>"
            query = query + "<ASSIGN VALUES HERE>"
            ## TO-DO: Assign which column element should be assigned for each column in the INSERT statement.
            ## For e.g., to INSERT artist_name and user first_name, you would change the code below to `line[0], line[1]`
            session.execute(query, (line[#], line[#]))





def test():
    # check the number of rows in your csv file
    if os.path.exists('event_datafile_new.csv'):
        with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:
            print(sum(1 for line in f))
    else:
        print('event datafile does not exist')



def main():
    #extract = Extract('event_data')
    #extract.save('event_datafile_new.csv')
    load()
    #test()


if __name__ == "__main__":
    main()
