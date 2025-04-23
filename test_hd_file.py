#!/usr/bin/env python3
import json
import csv
import os
from json_to_csv_converter import standardize_record, STANDARD_HEADERS

def test_hd_file(file_path):
    """Test direct processing of a Home Depot file."""
    print(f"Testing direct processing of {file_path}...")
    
    try:
        # Load the file
        with open(file_path, 'r', encoding='utf-8') as f:
            json_data = json.load(f)
        
        print(f"Loaded JSON data with keys: {list(json_data.keys())}")
        
        # Check if this is a product file
        if 'product' in json_data and isinstance(json_data['product'], dict):
            product = json_data['product']
            print(f"Found product data with keys: {list(product.keys())}")
            
            # Check for buybox_winner
            if 'buybox_winner' in json_data and isinstance(json_data['buybox_winner'], dict):
                buybox = json_data['buybox_winner']
                print(f"Found buybox_winner with keys: {list(buybox.keys())}")
                if 'price' in buybox:
                    print(f"Found buybox_winner.price: {buybox['price']}")
            else:
                print("No buybox_winner found at top level")
                
                # Check for nested buybox_winner
                if 'buybox_winner' in product and isinstance(product['buybox_winner'], dict):
                    buybox = product['buybox_winner']
                    print(f"Found product.buybox_winner with keys: {list(buybox.keys())}")
                    if 'price' in buybox:
                        print(f"Found product.buybox_winner.price: {buybox['price']}")
                else:
                    print("No buybox_winner found in product")
            
            # Standardize the record
            standardized = standardize_record(product)
            print(f"Standardized record: {standardized}")
            
            # Check if price was extracted
            if standardized.get('Rate'):
                print(f"SUCCESS: Price extracted as Rate: {standardized['Rate']}")
            else:
                print("ERROR: No price extracted")
                
            # Write to CSV
            output_file = 'test_hd_output.csv'
            with open(output_file, 'w', newline='', encoding='utf-8-sig') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=STANDARD_HEADERS)
                writer.writeheader()
                writer.writerow(standardized)
            
            print(f"CSV file created: {output_file}")
        else:
            print("ERROR: No product data found")
            
    except Exception as e:
        print(f"ERROR: {str(e)}")
    
    print("Test complete.")

if __name__ == "__main__":
    # Test with a Home Depot file
    hd_file = 'uploads/homedepot_raw_product_100000045.json'
    if os.path.exists(hd_file):
        test_hd_file(hd_file)
    else:
        print(f"File not found: {hd_file}")
