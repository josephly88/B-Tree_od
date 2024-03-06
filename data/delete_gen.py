import random
import os

length=30000

def shuffle_lines(file_path):
    with open(file_path, 'r') as file:
        lines = file.readlines()
        lines = [line.strip() for line in lines]
        lines[15:length+15] = random.sample(lines[15:length+15], k=len(lines[15:length+15]))
        for i in range(15, length+15):
            lines[i] = lines[i].replace("INSERT", "DELETE")
        return lines

def main():
    output_file = 'micro_d_30K.txt'
    input_file = 'micro_i_30K.txt'

    if not os.path.exists(output_file):
        with open(output_file, 'w') as file_W:
            pass

    shuffled_lines = shuffle_lines(input_file)

    with open(output_file, 'w') as file_W:
        file_W.write('\n'.join(shuffled_lines) + '\n')

if __name__ == "__main__":
    main()
