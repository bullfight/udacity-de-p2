import os
import glob
import pandas

class Extract:
    def __init__(self, directory):
        """Initialize Extract and set a directory(string)"""
        self.directory = directory

    def save(self, filename, columns=None, dropna=None):
        """Iterate over each file in the directory and save to filename

        Arguments:
        filename -- The output filename (string)
        columns  -- List of columns to extract from csv files (list), optional
        dropna   -- List of columns to drop rows when value is na (list), optional
        """
        filepaths   = self.__get_filepaths()
        data_frame  = self.__extract(filepaths)
        self.__export(data_frame, filename, columns, dropna)

    def __get_filepaths(self):
        return glob.glob(os.path.join(self.directory, '*.csv'))

    def __extract(self, filepaths):
        return pandas.concat(map(lambda file: pandas.read_csv(file, dtype=str), filepaths))

    def __export(self, data_frame, filename, columns, dropna):
        if columns:
            data_frame = data_frame[columns]

        if dropna:
            data_frame = data_frame.dropna(subset=dropna)

        data_frame.to_csv(filename, index=False)
