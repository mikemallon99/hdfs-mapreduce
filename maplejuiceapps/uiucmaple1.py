# Output # of apartments in champaign with > 30 units
def maple(key_value_pairs):
    output_pairs = []
    for key, values in key_value_pairs:
        if int(values) > 30:
            output_pairs.append([1, values])
    return output_pairs

def map_format(string_list):
    output_lines = []
    for string in string_list:
        key = string.split(",")[0]
        units = string.split(",")[3].replace(" ", "")
        if units == "":
            continue
        output_lines.append([key, units])
    return output_lines