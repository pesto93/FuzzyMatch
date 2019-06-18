# author_ = "Johnleonard C.O"
# Company_ = "Geophy"

from fuzzywuzzy import fuzz
import pandas as pd
import os
import csv
import re


class Fuzzy:

    def __init__(self):
        self.file_in_path = 'C:/Users/Johnleonard/Desktop/US_Counties/1_FuzzyWuzzy/fuzzy-string-matching/Files/'
        self.output_filename = 'C:/Users/Johnleonard/Desktop/US_Counties/1_FuzzyWuzzy/fuzzy-string-matching/Output/Sample.csv'
        self.address_names_in_file = ['representation', 'Street Address']  # please specify the fixed address columns names in each file here.
        self.address_match_dict = {}
        self.data_in_file_list = []
        self.data_in_file_dict = {}
        self.file_name_index = []
        self.result_row = []
        self.counter = 0
        self.index_of_li = ""
        self.row_Count = []

    def load_csv(self):
        for filename in os.listdir(self.file_in_path):
            print(" Reading " + filename)
            df = pd.read_csv(open(self.file_in_path + filename, "r", encoding="utf-8"), encoding='utf-8', low_memory=False)
            self.data_in_file_list.append(df)
            self.data_in_file_dict[filename] = df
            common = list(set(self.address_names_in_file).intersection(df.columns))  # Loops through the address list and df columns
            if len(common) > 0:
                adr = pd.Series(df[common[0]])
                self.address_match_dict[filename] = adr.dropna()
                self.file_name_index.append(filename)
                # series = pd.Series(df['Address'])

        # choose which function to use - between count_file_row() and loop_all_files()
        # self.count_file_row()
        self.loop_all_files()

#   ----------------------------------------------------------------
    ''''
        this function  when called inside - load_csv (), allows you to string match with only one file in mind which is the best file.  this function 
        counts all the rows in each file in the dir to determine which one of the them is the biggest and then after saves the biggest file as the 
        main file to string match every other file in the dir with.
        
        So you will have to choose between count_file_row() and loop_all_files() to determine which function serves your need the best. Please read 
        about - loop_all_files() -  
    '''
    def count_file_row(self):
        y = 0
        for x in range(len(self.data_in_file_list)):
            i = len(self.data_in_file_list[x])
            if i > y:
                y = i
                # print(y)
                self.index_of_li = x

        address_biggest_file = self.address_match_dict.get(self.file_name_index[self.index_of_li])
        print(" Running String Lookup with " + self.file_name_index[self.index_of_li] + " as the file with biggest - (" + str(
            y) + ") row number")
        # print(data_in_file_dict.get("master_sales_clean.csv"))  # print(address_biggest_file.index, address_biggest_file.values)

        self.compare_files(address_biggest_file)

#   ----------------------------------------------------------------
    ''''
        this function  when called inside - load_csv (), allows you to string match all the file one after the other. Meaning - it takes the first 
        file and loops it against all other file in the dir. Once its done, it takes the second file and does same until each file have been used 
        to compare every other file in the dir.

        So you will have to choose between count_file_row() and loop_all_files() to determine which function serves your need the best. Please read 
        about - count_file_row() -  
    '''
    def loop_all_files(self):
        for self.index_of_li in range(len(self.data_in_file_list)):
            look_up_file = self.address_match_dict.get(self.file_name_index[self.index_of_li])
            print(" Running String Lookup with " + self.file_name_index[self.index_of_li] + " with every other file")
            self.compare_files(look_up_file)

#   ----------------------------------------------------------------

    def compare_files(self, address_biggest_file):
        for i in range(len(self.data_in_file_list)):
            threshold = 90  # set your preferred ratio that must be exceeded to capture the data
            if i == self.index_of_li:
                pass
            else:
                print(' checking ' + self.file_name_index[int(self.index_of_li)] + ' file on ' + self.file_name_index[i])
                for index, rows in zip(address_biggest_file.index, address_biggest_file.values):
                    other_file_in_directory = self.address_match_dict.get(self.file_name_index[i])
                    for index2, row2 in zip(other_file_in_directory.index, other_file_in_directory.values):
                        token_set_ratio = fuzz.token_set_ratio(rows, row2)
                        if token_set_ratio > threshold:
                            first_data = self.data_in_file_dict[self.file_name_index[int(self.index_of_li)]].iloc[index]
                            second_data = self.data_in_file_dict[self.file_name_index[i]].iloc[index2]
                            df2 = pd.concat([first_data, second_data]).sort_index()
                            # this sort is very important to make sure the data stays consistent irrespective of the where the position of
                            # the column that contains the data is.
                            first_data_building_number = ''.join(re.findall("\\d+", rows))
                            second_data_building_number = ''.join(re.findall("\\d+", row2))

                            if first_data_building_number == second_data_building_number:
                                columns = self.data_in_file_dict[self.file_name_index[int(self.index_of_li)]].columns.tolist() + \
                                    self.data_in_file_dict[self.file_name_index[i]].columns.tolist()
                                # print(sorted(columns))
                                # Sort columns very important too
                                val_result_row = [val for idx, val in df2.iteritems()]
                                if self.counter == 0:
                                    self.create_csv(sorted(columns), self.output_filename)    # creates the output file with the cols
                                    self.counter += 1
                                print('Match', token_set_ratio, '%', val_result_row)
                                self.save_csv(val_result_row, self.output_filename)
                            else:
                                pass

        self.deduplicate_output_file()  # Remove duplicate rows in the output file.

    def deduplicate_output_file(self):
        df = pd.read_csv(self.output_filename, sep=";")
        # Notes:
        # - the `subset=None` means that every column is used
        #    to determine if two rows are different; to change that specify
        #    the columns as an array
        # - the `inplace=True` means that the data structure is changed and
        #   the duplicate rows are gone
        df.drop_duplicates(subset=None, inplace=True)

        # Write the results to a different file
        df.to_csv(self.output_filename, index=False, sep=";", quoting=csv.QUOTE_ALL)
        print("Duplicates rows removed and file is ready to be used.")

    @staticmethod
    def create_csv(main_data, output_file):
        if os.path.isfile(output_file):
            print('Result file already exist')
        else:
            print('Creating result file')
            with open(output_file, 'w+') as myfile:
                wr = csv.writer(myfile, quoting=csv.QUOTE_ALL, delimiter=";", lineterminator="\n")
                wr.writerow(main_data)

    @staticmethod
    def save_csv(main_data, output_file):
        with open(output_file, 'a') as myfile:
            try:
                wr = csv.writer(myfile, quoting=csv.QUOTE_ALL, delimiter=";", lineterminator="\n")
                wr.writerow(main_data)
            except:
                print('Error while adding new line')


my_fuzzy_starter = Fuzzy()
my_fuzzy_starter.load_csv()

