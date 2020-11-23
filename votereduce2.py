def reduce(key, value):
    cand_dict = {}
    for pair in value:
        key_a = pair.split('-')[0]
        key_b = pair.split('-')[1]
        cand_dict[key_a] = cand_dict.get(key_a, 0) + 1

    top_cand = None
    top_cand_list = []
    for cand in cand_dict.keys():
        if cand_dict[cand] > cand_dict.get(top_cand, 0):
            top_cand = cand
            top_cand_list = [cand]
        elif cand_dict[cand] == cand_dict.get(top_cand, 0):
            top_cand_list.append(cand)

    return [['winner', top_cand_list]]