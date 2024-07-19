#!/usr/bin/python3

import random
import os

length=2500000
input_file = 'insert_2_5M.txt'
output_file = 'delete_2_5M.txt'

def shuffle_lines(file_path):
    with open(file_path, 'r') as file:
        lines = file.readlines()
        lines = [line.strip() for line in lines]
        lines[15:length+15] = random.sample(lines[15:length+15], k=len(lines[15:length+15]))
        for i in range(15, length+15):
            lines[i] = lines[i].replace("INSERT", "DELETE")
        return lines

def main():
    if not os.path.exists(output_file):
        with open(output_file, 'w') as file_W:
            pass

    shuffled_lines = shuffle_lines(input_file)

    with open(output_file, 'w') as file_W:
        file_W.write('\n'.join(shuffled_lines) + '\n')

if __name__ == "__main__":
    main()
