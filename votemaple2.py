def maple(key, value):
    key_list = []
    for ind_value in value:
        key_list.append([1, key + '-' + ind_value])
    return key_list