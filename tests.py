import argparse
import logging
from maplejuice.maplejuice_worker import split_input_files, get_key_from_in_filename, run_maple_on_files, \
    get_prefix_from_out_filename, combine_key_files, split_juice_keys, get_key_from_juice_input, run_juice_on_files, \
    get_dest_prefix, combine_juice_output
import subprocess
import sys


def test_split():
    # create 2 files with 100 lines
    test_file_1 = open("prefix_test1", "w")
    test_file_2 = open("prefix_test2", "w")

    test_file_1.write("file 1: line # 0")
    test_file_2.write("file 2: line # 0")
    for i in range(1, 50):
        line = "line # " + str(i)
        test_file_1.write("\nfile 1: " + line)
        test_file_2.write("\nfile 2: " + line)

    test_file_1.close()
    test_file_2.close()

    file_list = ["prefix_test1", "prefix_test2"]
    machine_list = ["machine_1", "machine_2"]
    split_input_files(file_list, machine_list, "prefix_")

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


def test_get_key():
    print(get_key_from_in_filename("test1_block_2.txt"))
    print(get_key_from_in_filename("t_e_s_t_1_block_2.txt"))
    print(get_key_from_in_filename("test1__block_2.txt"))
    print(get_key_from_in_filename("__test1_block_2.txt"))
    return "DONE"


def test_run_maple():
    maple_exe = "sample"
    file_list = ["test1_block_1.txt", "test2_block_1.txt"]
    file_prfx = "intermediate_prefix"
    machine_id = 'fa20-cs425-g49-01'
    run_maple_on_files(maple_exe, file_list, file_prfx, machine_id)

    file_list = ["test1_block_2.txt", "test2_block_2.txt"]
    file_prfx = "intermediate_prefix"
    machine_id = 'fa20-cs425-g49-02'
    run_maple_on_files(maple_exe, file_list, file_prfx, machine_id)
    return "DONE"


def test_prefix():
    filename = "intermediate_prefix_key1_fa20-cs425-g49-01.cs.illinois.edu"
    get_prefix_from_out_filename(filename)
    return "DONE"


def test_combine():
    key_map = {"key1": ['intermediate_prefix_key1_fa20-cs425-g49-01', 'intermediate_prefix_key1_fa20-cs425-g49-01'],
               "key2": ['intermediate_prefix_key2_fa20-cs425-g49-01', 'intermediate_prefix_key2_fa20-cs425-g49-01']}
    combine_key_files(key_map)
    return "DONE"


def test_juice_split():
    key_list = ['intermediate_prefix_key1',
                'intermediate_prefix_key2',
                'intermediate_prefix_key3',
                'intermediate_prefix_key4']
    machines = ['fa20-cs425-g49-01', 'fa20-cs425-g49-02', 'fa20-cs425-g49-03']
    dict_ = split_juice_keys(key_list, machines)
    for key in dict_.keys():
        print(key+" :"+str(dict_[key]))
    return "DONE"


def test_juice_get_keyname():
    samplefile = "___intermediate___p_rfgedfix_tkghest_fds_fsd_key1"
    prefix = "___intermediate___p_rfgedfix_tkghest_fds_fsd"
    key = get_key_from_juice_input(samplefile, prefix)
    print("Filename: "+samplefile)
    print("Key: "+key)
    return "DONE"


def test_juice_run():
    juice_exe = "sample"
    file_list = ["intermediate_prefix_key1",
                 "intermediate_prefix_key2"]
    int_prefix = "intermediate_prefix"
    dest_prefix = "dest_prefix"
    machine_id = "fa20-cs425-g49-01"
    outfile_name = run_juice_on_files(juice_exe, file_list, int_prefix, dest_prefix, machine_id)
    print(outfile_name)
    return "DONE"


def test_dest_name():
    intermediate_dest = "dest_prefix_key1_fa20-cs425-g49-01"
    dest = get_dest_prefix(intermediate_dest)
    print("Destination name: "+dest)
    return "DONE"


def test_tuple():
    s = "('key', 1)"
    t = tuple(s)
    print(t)
    return "DONE"


def test_juice_combine():
    file_list = ["dest_prefix_fa20-cs425-g49-01", "dest_prefix_fa20-cs425-g49-02"]
    outfile = combine_juice_output(file_list)
    print(outfile)
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
    parser.add_argument("--keyname", default=False, action="store_true")
    parser.add_argument("--m_run", default=False, action="store_true")
    parser.add_argument("--prfx", default=False, action="store_true")
    parser.add_argument("--combine", default=False, action="store_true")
    parser.add_argument("--j_split", default=False, action="store_true")
    parser.add_argument("--j_key", default=False, action="store_true")
    parser.add_argument("--j_run", default=False, action="store_true")
    parser.add_argument("--dest", default=False, action="store_true")
    parser.add_argument("--tuple", default=False, action="store_true")
    parser.add_argument("--j_combine", default=False, action="store_true")

    p_args = parser.parse_args()

    ret = None
    if p_args.m_split:
        ret = test_split()
    elif p_args.subprocess:
        ret = test_subprocess()
    elif p_args.keyname:
        ret = test_get_key()
    elif p_args.m_run:
        ret = test_run_maple()
    elif p_args.prfx:
        ret = test_prefix()
    elif p_args.combine:
        ret = test_combine()
    elif p_args.j_split:
        ret = test_juice_split()
    elif p_args.j_key:
        ret = test_juice_get_keyname()
    elif p_args.j_run:
        ret = test_juice_run()
    elif p_args.dest:
        ret = test_dest_name()
    elif p_args.tuple:
        ret = test_tuple()
    elif p_args.j_combine:
        ret = test_juice_combine()
    else:
        ret = "No test ran"
    print(ret)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    parse_args()
