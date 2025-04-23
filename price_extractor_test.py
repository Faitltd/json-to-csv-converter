#!/usr/bin/env python3
import json
import glob
import csv
import re

def extract_price(value):
    """Extract a price value from various formats."""
    if isinstance(value, (int, float)):
        return str(value)

    if isinstance(value, str):
        # Try to extract numeric value from string (e.g., "$10.99")
        match = re.search(r'\$?([\d,]+\.?\d*)', value)
        if match:
            # Remove commas and convert to string
            return match.group(1).replace(',', '')

    # Return as is if we can't extract a price
    return str(value)

def find_price_in_json(json_data, path=""):
    """Recursively search for price fields in JSON data."""
    prices = []
    
    if isinstance(json_data, dict):
        for key, value in json_data.items():
            current_path = f"{path}.{key}" if path else key
            
            # Check if this is a price field
            if key == "price" and (isinstance(value, (int, float)) or 
                                  (isinstance(value, str) and re.search(r'\$?\d+\.?\d*', value))):
                prices.append((current_path, value))
            
            # Recursively search nested dictionaries and lists
            if isinstance(value, (dict, list)):
                prices.extend(find_price_in_json(value, current_path))
    
    elif isinstance(json_data, list):
        for i, item in enumerate(json_data):
            current_path = f"{path}[{i}]"
            if isinstance(item, (dict, list)):
                prices.extend(find_price_in_json(item, current_path))
    
    return prices

def main():
    # Find all Home Depot product files
    product_files = glob.glob('uploads/homedepot_raw_product_*.json')
    
    # Create CSV file for results
    with open('price_analysis.csv', 'w', newline='', encoding='utf-8-sig') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['File', 'Product ID', 'Product Name', 'Price Path', 'Price Value'])
        
        # Process each file
        for file_path in product_files[:10]:  # Process first 10 files
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    json_data = json.load(f)
                    
                    # Get product info
                    product_id = "Unknown"
                    product_name = "Unknown"
                    
                    if 'product' in json_data and isinstance(json_data['product'], dict):
                        product = json_data['product']
                        product_id = product.get('item_id', product.get('model_number', "Unknown"))
                        product_name = product.get('title', "Unknown")
                    
                    # Find all price fields
                    prices = find_price_in_json(json_data)
                    
                    if prices:
                        for price_path, price_value in prices:
                            writer.writerow([
                                file_path, 
                                product_id, 
                                product_name, 
                                price_path, 
                                extract_price(price_value)
                            ])
                    else:
                        writer.writerow([file_path, product_id, product_name, "No price found", ""])
                        
            except Exception as e:
                print(f"Error processing {file_path}: {str(e)}")
    
    print(f"Price analysis complete. Results saved to price_analysis.csv")

if __name__ == "__main__":
    main()
