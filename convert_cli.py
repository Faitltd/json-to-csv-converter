#!/usr/bin/env python3
import os
import sys
from json_to_csv_converter import combine_json_to_csv

def main():
    if len(sys.argv) < 3:
        print("Usage: python3 convert_cli.py <input_pattern> <output_file>")
        print("Example: python3 convert_cli.py 'data/*.json' 'output.csv'")
        return
    
    input_pattern = sys.argv[1]
    output_file = sys.argv[2]
    
    # Ensure output file has .csv extension
    if not output_file.lower().endswith('.csv'):
        output_file += '.csv'
    
    print(f"Converting JSON files matching '{input_pattern}' to CSV file '{output_file}'...")
    combine_json_to_csv(input_pattern, output_file)
    print(f"Conversion complete! CSV file saved as: {output_file}")
    
    # Verify the file was created with the correct extension
    if os.path.exists(output_file):
        print(f"File created successfully: {output_file}")
        print(f"File size: {os.path.getsize(output_file)} bytes")
    else:
        print(f"Error: File {output_file} was not created.")

if __name__ == "__main__":
    main()
