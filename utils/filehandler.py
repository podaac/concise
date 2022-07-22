"""Module for file handling calls"""

import json

from os import path


class FileHandler():
    """
    Class for file handling calls
    """

    def get_file_content(filename: str) -> str:
        """
        Function to get the content of a file
        """

        filepath = path.realpath(filename)
        if path.exists(filepath):
            result = open(filepath).read()
            return result
        else:
            raise FileExistsError(f"{filepath} does not exists!")

    def get_file_content_list_per_line(filename: str) -> list:
        """
        Function to get the content of a file, turned into a list.
        Each line is a list element
        """

        filepath = path.realpath(filename)
        if path.exists(filepath):
            with open(filepath) as fileContent:
                try:
                    result = json.load(fileContent)
                except ValueError:
                    result = []
                    fileContent.seek(0)
                    lines = fileContent.readlines()
                    for line in lines:
                        result.append(line.strip())
            return result
        else:
            raise FileExistsError(f"{filepath} does not exists!")
