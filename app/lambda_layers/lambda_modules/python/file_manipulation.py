# Purpose: Python functions to manipulate files and directories
# Author: Brian Piperato
# Created on: 10/8/2019
# Last Modified on: 10/17/2019

import sys
import zipfile
import os
import pathlib
import glob
import threading


def filename(file_path, file_rename):
    # Retrieve file name by getting full file path and stripping parent directories
    path = pathlib.Path(file_path).parent
    updated_file_path = os.path.join(path, file_rename)
    os.rename(file_path, updated_file_path)
    return updated_file_path


def directory_change(file_path, new_file_path):
    os.replace(file_path, new_file_path)
    return 'File Moved'


def read_directory(directory):
    directory_list = [i for i in glob.glob(directory+'*.*')]
    return directory_list


def data_unzip_and_move(directory):
    for i in read_directory(directory):
        orig_filename = os.path.basename(i)
        if orig_filename.startswith('On_Time_Reporting') and orig_filename.endswith('.zip'):
            while os.stat(i).st_size == 0:
                print('Waiting for zip to finish downloading', end='\r')
            if '.csv' not in i:
                with zipfile.ZipFile(i, 'r') as zip_ref:
                    zip_ref.extractall(directory)
    for k in read_directory(directory):
        if k.endswith('csv'):
            k2 = k.replace('On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_', '')
            os.rename(k, k2)


class ProgressPercentage(object):
    def __init__(self, file_name):
        self._filename = file_name
        self._size = float(os.path.getsize(file_name))
        self.seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        with self._lock:
            self.seen_so_far += bytes_amount
            percentage = (self.seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s %s / %s (%.2f%%)" % (
                    self._filename, self.seen_so_far, self._size, percentage
                )
            )
            sys.stdout.flush()
