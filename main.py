import random


def generate_name() -> str:
    new_name = ''
    name_file = open("resources/names.txt")
    num_lines = sum(1 for line in name_file)
    first_name_index = random.randrange(0, num_lines)
    last_name_index = random.randrange(0, num_lines)
    while last_name_index == first_name_index:
        last_name_index = random.randrange(0, num_lines)

    name_file.seek(0)

    for index, line in enumerate(name_file):
        if index == first_name_index:
            new_name += str.rstrip(line)
        elif index == last_name_index:
            new_name += str.rstrip(line)

    name_file.close()
    return new_name


while True:
    print(generate_name())
