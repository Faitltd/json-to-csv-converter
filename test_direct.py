#!/usr/bin/env python3
import json
from json_to_csv_converter import standardize_record

def test_direct_processing():
    """Test direct processing of a test file."""
    print("Testing direct processing of test_product.json...")
    
    try:
        # Load the test file
        with open('test_product.json', 'r', encoding='utf-8') as f:
            json_data = json.load(f)
        
        print(f"Loaded JSON data: {json_data}")
        
        # Check if this is a product file
        if 'product' in json_data and isinstance(json_data['product'], dict):
            product = json_data['product']
            print(f"Found product data: {product}")
            
            # Standardize the record
            standardized = standardize_record(product)
            print(f"Standardized record: {standardized}")
            
            # Check if price was extracted
            if standardized.get('Rate'):
                print(f"SUCCESS: Price extracted as Rate: {standardized['Rate']}")
            else:
                print("ERROR: No price extracted")
        else:
            print("ERROR: No product data found")
            
    except Exception as e:
        print(f"ERROR: {str(e)}")
    
    print("Test complete.")

if __name__ == "__main__":
    test_direct_processing()
