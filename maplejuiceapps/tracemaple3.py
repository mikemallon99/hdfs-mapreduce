def maple(key_value_pairs):
    output_pairs = []
    for key, values in key_value_pairs:
        values_split = values.split(",")
        name = values_split[0]
        output_pairs.append([name, values[len(name):]])
    return output_pairs

def map_format(string_list):
    output_lines = []
    for string in string_list:
        key = string.split("|")[0]
        value = string.split("|")[1]
        value = value.replace("\n", "")
        output_lines.append([key, value])
    return output_lines