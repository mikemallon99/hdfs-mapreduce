import random

def maple(key_value_pair):
    return_list = []
    for key, values in key_value_pair:
        # Take each string and interpet it as a vote
        votes = values.split(',')
        for i in range(0,2):
            for j in range(i+1, 3):
                if votes[i] < votes[j]:
                    return_list.append([votes[i]+'-'+votes[j], 1])
                else:
                    return_list.append([votes[j]+'-'+votes[i], 0])
    return return_list

def map_format(string_list):
    output_lines = []
    for string in string_list:
        key = string.split("|")[0]
        value = string.split("|")[1]
        value.replace("\n", "")
        output_lines.append([key, value])
    return output_lines

def generate_testfiles(files):
    candidates = ['c1', 'c2', 'c3']

    for i in range(0, files):
        f = open('votesample' + str(i), 'w')
        for line in range(0, 25):
            random_ranking = random.sample(candidates, 3)
            f.write("key|")
            for cand in random_ranking:
                f.write(cand + ',')
            f.write('\n')
        f.close()