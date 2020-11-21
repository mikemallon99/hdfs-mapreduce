import argparse
import logging
from maplejuice.maplejuice_worker import split_files_among_machines
import subprocess
import sys


def test_split():
    # create 2 files with 100 lines
    test_file_1 = open("test1.txt", "w")
    test_file_2 = open("test2.txt", "w")

    test_file_1.write("file 1: line # 0")
    test_file_2.write("file 2: line # 0")
    for i in range(1, 50):
        line = "line # " + str(i)
        test_file_1.write("\nfile 1: " + line)
        test_file_2.write("\nfile 2: " + line)

    test_file_1.close()
    test_file_2.close()

    file_list = ["test1.txt", "test2.txt"]
    machine_list = ["machine_1", "machine_2", "machine_3"]
    rv = split_files_among_machines(file_list, machine_list)

    for m in rv.keys():
        msg = m + ": " + str(rv[m])
        print(msg)

    return "DONE"


def test_subprocess():
    python_exe = "sample"
    __import__(python_exe)
    maple = sys.modules[python_exe]
    if maple.run():
        print("Script completed!")
    else:
        return "ERROR"
    return "DONE"


def parse_args():
    """
    parse the CLI arguments
    """
    description = '''
                Unit tests for maplejuice
                '''
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--m_split", default=False, action="store_true")
    parser.add_argument("--subprocess", default=False, action="store_true")

    p_args = parser.parse_args()

    ret = None
    if p_args.m_split:
        ret = test_split()
    elif p_args.subprocess:
        ret = test_subprocess()
    else:
        ret = "No test ran"
    print(ret)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    parse_args()
