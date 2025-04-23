#!/usr/bin/env python3
import json
import csv
from json_to_csv_converter import standardize_record, STANDARD_HEADERS

def test_csv_output():
    """Test direct processing of a test file and writing to CSV."""
    print("Testing CSV output with test_product.json...")
    
    try:
        # Load the test file
        with open('test_product.json', 'r', encoding='utf-8') as f:
            json_data = json.load(f)
        
        # Check if this is a product file
        if 'product' in json_data and isinstance(json_data['product'], dict):
            product = json_data['product']
            
            # Standardize the record
            standardized = standardize_record(product)
            
            # Write to CSV
            with open('test_output.csv', 'w', newline='', encoding='utf-8-sig') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=STANDARD_HEADERS)
                writer.writeheader()
                writer.writerow(standardized)
            
            print(f"CSV file created: test_output.csv")
            
            # Read back the CSV file
            with open('test_output.csv', 'r', encoding='utf-8-sig') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    print(f"CSV row read back: {row}")
                    if row.get('Rate'):
                        print(f"SUCCESS: Price found in CSV: {row['Rate']}")
                    else:
                        print("ERROR: No price in CSV")
        else:
            print("ERROR: No product data found")
            
    except Exception as e:
        print(f"ERROR: {str(e)}")
    
    print("Test complete.")

if __name__ == "__main__":
    test_csv_output()
