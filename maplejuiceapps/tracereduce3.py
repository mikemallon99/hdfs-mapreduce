def juice(key, value_list):
    output_string = ""
    covid_status = "negative"
    for value in value_list:
        if "positive" in value:
            covid_status = "positive"

    for value in value_list:
        if value == "positive" or value == "negative":
            continue
        output_string += value + "," + covid_status + "/"
    return [key, output_string]