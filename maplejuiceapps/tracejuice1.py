# Identity
def juice(key, value_list):
    output_string = ""
    for value in value_list:
        output_string += value + '/'

    return [key, output_string]