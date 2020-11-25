import random

# Read from D1 and output (key=name, value=(location,start,end))
def maple(key_value_pairs):
    output_pairs = []
    for key, values in key_value_pairs:
        values_split = values.split(",")
        name = values_split[0]
        location = values_split[1]
        start_time = values_split[2]
        end_time = values_split[3]
        value_string = f"{location},{start_time},{end_time}"
        output_pairs.append([name, value_string])
    return output_pairs

def map_format(string_list):
    output_lines = []
    for string in string_list:
        key = string.split("|")[0]
        value = string.split("|")[1]
        value = value.replace("\n", "")
        output_lines.append([key, value])
    return output_lines

def generate_testfiles(files):
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


    for i in range(0, files):
        f = open('traceD1sample' + str(i), 'w')
        for line in range(0, 25):
            random_name = random.sample(names, 1)
            random_location = random.sample(locations, 1)
            random_start = random.randint(1,24)
            random_end = random.randint(1,3) + random_start
            if random_end > 24:
                random_end = 24
            f.write(f"key|{random_name},{random_location},{random_start},{random_end}")
            f.write('\n')
        f.close()