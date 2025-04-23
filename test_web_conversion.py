#!/usr/bin/env python3
import json
import glob
import os
import csv
from json_to_csv_converter import process_json_file

def test_web_conversion():
    """Test the conversion process used by the web application."""
    # Find all JSON files in the uploads directory
    json_files = glob.glob('uploads/*.json')
    
    print(f"Testing conversion on {len(json_files)} files...")
    
    # Dictionary to track unique records
    known_records = {}
    
    # Create CSV file for results
    with open('test_web_output.csv', 'w', newline='', encoding='utf-8-sig') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['File', 'Product ID', 'Product Name', 'Rate'])
        
        # Process each file
        total_records = 0
        total_with_price = 0
        
        for file_path in json_files:
            try:
                # Process the file using the same function as the web application
                standardized_data, record_count, duplicate_count, errors = process_json_file(file_path, known_records)
                
                total_records += record_count
                
                # Check each record for a price
                for record in standardized_data:
                    product_id = record.get('Item ID', 'Unknown')
                    product_name = record.get('Item Name', 'Unknown')
                    rate = record.get('Rate', '')
                    
                    writer.writerow([
                        os.path.basename(file_path),
                        product_id,
                        product_name,
                        rate
                    ])
                    
                    if rate:
                        total_with_price += 1
                    
            except Exception as e:
                print(f"Error processing {file_path}: {str(e)}")
    
    print(f"\nConversion test complete.")
    print(f"Total records processed: {total_records}")
    print(f"Records with price: {total_with_price}")
    print(f"Percentage with price: {(total_with_price / total_records * 100) if total_records > 0 else 0:.2f}%")
    print(f"Results saved to test_web_output.csv")

if __name__ == "__main__":
    test_web_conversion()
