"""
"""

import os
import sys


def split_file(dir_name, full_file_name, limit=0):
    parts = full_file_name.split(".")
    file_name = ".".join(parts[:-1])
    file_ending = parts[-1]
    month_file_handlers = dict()
    k = 0
    base_file_path = os.path.join(dir_name, full_file_name)
    with open(base_file_path) as f:
        print("open file", base_file_path)
        header = f.readline()
        for row in f:  # expect row to start with a yyyy-mm-dd date, like '2016-01-12'
           k += 1
           if limit != 0 and k >= limit:
               print("reached limit", k)
               break
           year_and_month = row[:7]
           if year_and_month not in month_file_handlers:
               month_file_name = file_name + "_" + year_and_month + "." + file_ending
               print("create file", month_file_name)
               month_file_handlers[year_and_month] = open(os.path.join(dir_name, month_file_name), "w")
               month_file_handlers[year_and_month].write(header)
           month_file_handlers[year_and_month].write(row)
           if k % 1000000 == 0:
               print("reached row", k)
    for handle in month_file_handlers.values():
        handle.close()


def demo():
    dir_name = "/export/scratch/1kastner/neural_networks/"
    file_name = "evaluation_data_husconet.csv"
    split_file(
        dir_name, 
        file_name, 
        limit=3  # for testing purposes
    )


if __name__ == "__main__":
    print("usage: <script> <directory> <file1> <file2> ...")
    if len(sys.argv) > 2:
        dir_name = sys.argv[1]
        file_names = sys.argv[2:]
        for file_name in file_names:
            split_file(dir_name, file_name)
    else:
        demo()
