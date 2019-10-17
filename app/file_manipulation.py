# Purpose: Python functions to manipulate files and directories
# Author: Brian Piperato
# Created on: 10/8/2019
# Last Modified on: 10/8/2019


import os
import pathlib
import glob


def filename(file_path, file_rename):
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
