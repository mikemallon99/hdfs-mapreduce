import random

def maple(key_value_pairs):
    output_pairs = []
    for key, values in key_value_pairs:
        value_split = values.split(",")
        name = value_split[0]
        covid_status = value_split[1]
        output_pairs.append([name, covid_status])
    return output_pairs

def map_format(string_list):
    output_lines = []
    for string in string_list:
        key = string.split("|")[0]
        value = string.split("|")[1]
        value = value.replace("\n", "")
        output_lines.append([key, value])
    return output_lines

def generate_testfiles():
    names = [
        'mike', 'robbie', 'jack', 'luke', 'nate', 'bryan',
        'ben', 'rachel', 'nicole', 'gita', 'alex', 'aileen',
        'alejandro', 'hayley', 'zoe', 'obama', 'joe'
    ]
    locations = [
        'quad', 'ike', 'allen', 'isr', 'eceb',
        'ugl', 'grainger', 'oaklawn', 'chicago', 'evanston'
    ]
    times = range(1, 25)
    covid = {"positive", "negative"}

    f = open('traceD2sample', 'w')
    for line in range(0, len(names)):
        random_name = names[line]
        covid_status = covid[random.randint(0,1)]
        if random_end > 24:
            random_end = 24
        f.write(f"key|{random_name},{covid_status}")
        f.write('\n')
    f.close()