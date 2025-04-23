#!/usr/bin/env python3
import json
import glob
import os
from json_to_csv_converter import standardize_record

def test_price_extraction():
    """Test price extraction from Home Depot JSON files."""
    # Find all Home Depot product files
    product_files = glob.glob('uploads/homedepot_raw_product_*.json')
    
    print(f"Testing price extraction on {len(product_files)} files...")
    
    # Process each file
    for i, file_path in enumerate(product_files[:5]):  # Process first 5 files
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                json_data = json.load(f)
                
                # Get product info
                if 'product' in json_data and isinstance(json_data['product'], dict):
                    product = json_data['product'].copy()
                    
                    # Add buybox_winner if it exists at the top level
                    if 'buybox_winner' in json_data and isinstance(json_data['buybox_winner'], dict):
                        product['buybox_winner'] = json_data['buybox_winner']
                    
                    # Get product ID and name
                    product_id = product.get('item_id', product.get('model_number', "Unknown"))
                    product_name = product.get('title', "Unknown")
                    
                    print(f"\nFile {i+1}: {os.path.basename(file_path)}")
                    print(f"  Product ID: {product_id}")
                    print(f"  Product Name: {product_name}")
                    
                    # Check for buybox_winner price
                    if 'buybox_winner' in product and isinstance(product['buybox_winner'], dict):
                        if 'price' in product['buybox_winner']:
                            print(f"  Raw buybox_winner.price: {product['buybox_winner']['price']}")
                    
                    # Standardize the record
                    standardized = standardize_record(product)
                    print(f"  Standardized Rate: {standardized.get('Rate', 'Not found')}")
                    
                else:
                    print(f"\nFile {i+1}: {os.path.basename(file_path)} - No product data found")
                    
        except Exception as e:
            print(f"\nFile {i+1}: {os.path.basename(file_path)} - Error: {str(e)}")
    
    print("\nPrice extraction test complete.")

if __name__ == "__main__":
    test_price_extraction()
