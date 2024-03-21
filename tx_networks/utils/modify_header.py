import argparse


def switch_headers(input_file, output_file, new_header):
    with open(input_file, 'r') as old_f:
        with open(output_file, 'w') as new_f:
            switch = True
            for ln in old_f:
                if switch:
                    new_f.write(new_header + "\n")
                    switch = False
                else:
                    new_f.write(ln)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Change header of a CSV file')
    parser.add_argument('input_file', help='Path to the input CSV file')
    parser.add_argument('output_file', help='Path to the output CSV file')
    parser.add_argument('new_header', help='New header for the CSV file')
    args = parser.parse_args()

    switch_headers(args.input_file, args.output_file, args.new_header)
