import csv

# Configuration
INPUT_FILE = "train.csv"       # Path to the original CSV
OUTPUT_FILE = "cleaned_data.csv"  # Path to the cleaned CSV
EXPECTED_COLUMNS = [
    "row_id", "rank", "trade_min", "id_stock", "open", "close", "high", "low", "volume",
    "a1_v", "a2_v", "a3_v", "a4_v", "a5_v", "b1_v", "b2_v", "b3_v", "b4_v", "b5_v",
    "a1_p", "a2_p", "a3_p", "a4_p", "a5_p", "b1_p", "b2_p", "b3_p", "b4_p", "b5_p",
    "total_ask", "total_bid", "average_ask", "average_bid", "cjcs", "target"
]
EXPECTED_COLUMN_COUNT = len(EXPECTED_COLUMNS)

def validate_and_fix_csv(input_file, output_file):
    valid_rows = []
    total_rows = 0
    fixed_rows = 0

    with open(input_file, "r") as infile:
        reader = csv.reader(infile, delimiter="\t")  # Specify tab delimiter
        header = next(reader, None)
        total_rows += 1

        print(len(header))
        print(header)
        # Validate header
        if header and len(header) != EXPECTED_COLUMN_COUNT:
            print("Error: Header does not match expected column count.")
            return
        elif header != EXPECTED_COLUMNS:
            print("Warning: Header columns do not exactly match but will be written as expected.")

        valid_rows.append(EXPECTED_COLUMNS)  # Write expected header

        # Process each row
        for row in reader:
            total_rows += 1
            if len(row) == EXPECTED_COLUMN_COUNT:
                valid_rows.append(row)
            else:
                fixed_rows += 1

    # Write the cleaned CSV
    with open(output_file, "w", newline="") as outfile:
        writer = csv.writer(outfile, delimiter=",")  # Write as comma-separated
        writer.writerows(valid_rows)

    print(f"Total rows processed: {total_rows}")
    print(f"Rows fixed (removed or incomplete): {fixed_rows}")
    print(f"Cleaned data written to: {output_file}")

if __name__ == "__main__":
    validate_and_fix_csv(INPUT_FILE, OUTPUT_FILE)
