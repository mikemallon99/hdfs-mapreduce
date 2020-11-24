def juice(key, value):
    zero_count = 0
    one_count = 0
    for num in value:
        if int(num) == 0:
            zero_count += 1
        elif int(num) == 1:
            one_count += 1
    key_a = key.split('-')[0]
    key_b = key.split('-')[1]
    if one_count > zero_count:
        return [key_a, key_b]
    else:
        return [key_b, key_a]