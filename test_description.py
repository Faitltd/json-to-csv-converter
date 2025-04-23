#!/usr/bin/env python3
import json
import os

def find_description_fields(file_path):
    """Find all possible description fields in a Home Depot search results file."""
    print(f"Analyzing file: {file_path}")

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            json_data = json.load(f)

        if 'search_results' in json_data:
            search_results = json_data['search_results']

            # Handle different search results structures
            products = []

            # Check for products array in search_results
            if isinstance(search_results, dict) and 'products' in search_results and isinstance(search_results['products'], list):
                print(f"Found {len(search_results['products'])} products in search_results.products")
                products = search_results['products']
            # Check if search_results is itself an array of products
            elif isinstance(search_results, list):
                print(f"Found {len(search_results)} products in search_results array")
                products = search_results

            # Process first 5 products
            for i, product in enumerate(products[:5]):
                if isinstance(product, dict):
                    print(f"\nProduct {i+1}:")
                    print(f"  Keys at top level: {list(product.keys())}")

                    # Check for description fields at top level
                    for field in ['description', 'snippet', 'details', 'long_description', 'short_description', 'summary']:
                        if field in product:
                            print(f"  Found {field} at top level: {str(product[field])[:100]}...")

                    # Check for product data
                    if 'product' in product and isinstance(product['product'], dict):
                        product_data = product['product']
                        print(f"  Product title: {product_data.get('title', 'No title')}")
                        print(f"  Keys in product: {list(product_data.keys())}")

                        # Check for description fields in product
                        for field in ['description', 'snippet', 'details', 'long_description', 'short_description', 'summary']:
                            if field in product_data:
                                print(f"  Found {field} in product: {str(product_data[field])[:100]}...")

                    # Check for content_spec
                    if 'content_spec' in product and isinstance(product['content_spec'], dict):
                        print(f"  Keys in content_spec: {list(product['content_spec'].keys())}")
                        if 'description' in product['content_spec']:
                            print(f"  Found description in content_spec: {str(product['content_spec']['description'])[:100]}...")

                    # Check for specifications
                    if 'specifications' in product and isinstance(product['specifications'], list):
                        print(f"  Found {len(product['specifications'])} specifications")
                        for spec in product['specifications']:
                            if isinstance(spec, dict) and 'key' in spec and 'value' in spec:
                                print(f"  Specification: {spec['key']} = {str(spec['value'])[:50]}...")
                                if spec['key'] == 'Description':
                                    print(f"  Found Description in specifications: {str(spec['value'])[:100]}...")

                    # Check for overview
                    if 'overview' in product and isinstance(product['overview'], dict):
                        print(f"  Keys in overview: {list(product['overview'].keys())}")
                        if 'description' in product['overview']:
                            print(f"  Found description in overview: {str(product['overview']['description'])[:100]}...")
        else:
            print("No search_results found in file")

    except Exception as e:
        print(f"Error analyzing file: {str(e)}")

if __name__ == "__main__":
    # Test with multiple Home Depot search results files
    search_files = [
        'uploads/search_pvc_page_1.json',
        'uploads/search_plywood_page_1.json',
        'uploads/search_lumber_page_1.json'
    ]

    for search_file in search_files:
        if os.path.exists(search_file):
            find_description_fields(search_file)
            print("\n" + "-"*80 + "\n")
        else:
            print(f"File not found: {search_file}")
