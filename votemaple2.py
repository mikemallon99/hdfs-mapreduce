def maple(key_value_pair):
    key_list = []
    for key, value in key_value_pair:
        key_list.append([1, key + '-' + value])
    return key_list

def map_format(string_list):
    output_lines = []
    for string in string_list:
        key = string.split("|")[0]
        value = string.split("|")[1]
        output_lines.append([key, value])
    return output_lines