from os import path
from re import sub, compile, DOTALL
from typing import Dict, List

class QueryUtils:
    def __init__(self) -> None:
        pass

    def get_str_query_file(self, str_path_file):
        if not path.exists(str_path_file):
            print('Path not exist::{}'.format(str_path_file))
        elif not path.isfile(str_path_file):
            print('File not exist in path::{}'.format(str_path_file))
        else:
            try:
                    with open(str_path_file, 'r') as file:
                        str_file = file.read()
                        return str_file

            except IOError as e:
                print("I/O Erro({0}): {1}".format(e.errno, e.strerror))
                return None
            except:  # handle other exceptions such as attribute errors
                print('Check access grant file::{}'.format(str_path_file))
                return None

    def minify_query(self, str_query):
        data = sub(compile("/\*.*?\*/", DOTALL), "",
                      str_query)  # remove all occurrences streamed comments (/*COMMENT */) from string

        data = sub(compile("//.*?\n"), "",
                      data)  # remove all occurrence single-line comments (//COMMENT\n ) from string
        data = sub(compile("--.*?\n"), "",
                      data)  # remove all occurrence single-line comments (//COMMENT\n ) from string

        data = data.replace('\n', ' ')
        data = data.replace('\t', ' ')
        data = data.replace('  ', ' ')
        data = data.replace('   ', ' ')
        return data