import json
import csv
import glob
import os
import time
import logging
import re
from typing import List, Dict, Any, Tuple, Callable, Optional, Union
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Standard column headers and their mappings from various possible source fields
STANDARD_HEADERS = [
    "Item ID",
    "Item Name",
    "SKU",
    "Description",
    "Rate",
    "Source",
    "Reference ID",
    "Usage unit",
    "Purchase Rate",
    "Purchase Account",
    "Vendor",
    "Item Type",
    "Is Combo Product",
    "CF.Manufacturer",
    "CF.URL",
    "CF.Markup",
    "CF.Description",
    "CF.Supplier",
    "CF.Material",
    "CF.Cost"
]

# Mapping of source fields to standard fields
FIELD_MAPPING = {
    # Item ID mappings
    "id": "Item ID",
    "item_id": "Item ID",
    "itemId": "Item ID",
    "product_id": "Item ID",
    "productId": "Item ID",

    # Item Name mappings
    "name": "Item Name",
    "item_name": "Item Name",
    "itemName": "Item Name",
    "product_name": "Item Name",
    "productName": "Item Name",
    "title": "Item Name",

    # SKU mappings
    "sku": "SKU",
    "item_sku": "SKU",
    "itemSku": "SKU",
    "product_sku": "SKU",
    "productSku": "SKU",
    "model": "SKU",
    "model_number": "SKU",
    "modelNumber": "SKU",

    # Description mappings
    "description": "Description",
    "item_description": "Description",
    "itemDescription": "Description",
    "product_description": "Description",
    "productDescription": "Description",
    "details": "Description",
    "product_details": "Description",
    "productDetails": "Description",

    # Rate/Price mappings
    "price": "Rate",
    "rate": "Rate",
    "item_price": "Rate",
    "itemPrice": "Rate",
    "product_price": "Rate",
    "productPrice": "Rate",
    "unit_price": "Rate",
    "unitPrice": "Rate",

    # Reference ID mappings
    "reference_id": "Reference ID",
    "referenceId": "Reference ID",
    "ref_id": "Reference ID",
    "refId": "Reference ID",
    "external_id": "Reference ID",
    "externalId": "Reference ID",

    # Usage unit mappings
    "unit": "Usage unit",
    "usage_unit": "Usage unit",
    "usageUnit": "Usage unit",
    "measure_unit": "Usage unit",
    "measureUnit": "Usage unit",

    # Purchase Rate mappings
    "purchase_price": "Purchase Rate",
    "purchasePrice": "Purchase Rate",
    "cost_price": "Purchase Rate",
    "costPrice": "Purchase Rate",
    "wholesale_price": "Purchase Rate",
    "wholesalePrice": "Purchase Rate",

    # Item Type mappings
    "category": "Item Type",
    "item_type": "Item Type",
    "itemType": "Item Type",
    "product_type": "Item Type",
    "productType": "Item Type",
    "product_category": "Item Type",
    "productCategory": "Item Type",

    # Manufacturer mappings
    "manufacturer": "CF.Manufacturer",
    "brand": "CF.Manufacturer",
    "maker": "CF.Manufacturer",

    # URL mappings
    "url": "CF.URL",
    "product_url": "CF.URL",
    "productUrl": "CF.URL",
    "link": "CF.URL",
    "web_link": "CF.URL",
    "webLink": "CF.URL",

    # Material mappings
    "material": "CF.Material",
    "materials": "CF.Material",
    "item_material": "CF.Material",
    "itemMaterial": "CF.Material",
    "product_material": "CF.Material",
    "productMaterial": "CF.Material",
}

# Function to map source field names to standard field names
def map_field_name(field_name: str) -> str:
    """Map a source field name to a standard field name."""
    # Convert to lowercase for case-insensitive matching
    field_lower = field_name.lower()

    # Direct mapping
    if field_lower in FIELD_MAPPING:
        return FIELD_MAPPING[field_lower]

    # Try to match based on common patterns
    for source, target in FIELD_MAPPING.items():
        # Check if the field contains the source as a substring
        if source in field_lower:
            return target

    # If no mapping found, return the original field name
    return field_name

# Function to standardize a data record
def standardize_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """Standardize a data record by mapping fields and adding default values."""
    standardized = {header: "" for header in STANDARD_HEADERS}

    # Set default values
    standardized["Source"] = "HD"
    standardized["Purchase Account"] = "HD"
    standardized["Vendor"] = "HD"
    standardized["CF.Markup"] = "43%"
    standardized["CF.Supplier"] = "HD"

    # Extract product ID directly from item_id or model_number
    if 'item_id' in record:
        standardized["Item ID"] = str(record['item_id'])
    elif 'model_number' in record:
        standardized["Item ID"] = str(record['model_number'])

    # Extract SKU from model_number or store_sku
    if 'model_number' in record:
        standardized["SKU"] = str(record['model_number'])
    elif 'store_sku' in record:
        standardized["SKU"] = str(record['store_sku'])

    # Handle Home Depot specific structure for identifiers
    if 'identifiers' in record and isinstance(record['identifiers'], dict):
        # Extract product ID if not already set
        if not standardized["Item ID"]:
            if 'product_id' in record['identifiers']:
                standardized["Item ID"] = str(record['identifiers']['product_id'])
            elif 'item_id' in record['identifiers']:
                standardized["Item ID"] = str(record['identifiers']['item_id'])

        # Extract SKU/model if not already set
        if not standardized["SKU"]:
            if 'sku' in record['identifiers']:
                standardized["SKU"] = str(record['identifiers']['sku'])
            elif 'model_number' in record['identifiers']:
                standardized["SKU"] = str(record['identifiers']['model_number'])

    # Extract product name
    if 'title' in record:
        standardized["Item Name"] = str(record['title'])
    elif 'name' in record:
        standardized["Item Name"] = str(record['name'])

    # Extract description
    if 'description' in record and record['description']:
        standardized["Description"] = str(record['description'])
        standardized["CF.Description"] = str(record['description'])
    elif 'details' in record and record['details']:
        standardized["Description"] = str(record['details'])
        standardized["CF.Description"] = str(record['details'])
    # Use title as description if no description is found
    elif 'title' in record and record['title']:
        standardized["Description"] = str(record['title'])
        standardized["CF.Description"] = str(record['title'])
    elif standardized.get("Item Name"):
        standardized["Description"] = standardized["Item Name"]
        standardized["CF.Description"] = standardized["Item Name"]

    # PRICE EXTRACTION - SIMPLIFIED AND FOCUSED ON KNOWN PATHS
    price_value = None

    # Log price extraction attempt
    logger.info(f"  Price extraction for product: {standardized.get('Item Name', 'Unknown')}")

    # Direct approach for product.buybox_winner.price (most common path based on analysis)
    if 'buybox_winner' in record and isinstance(record['buybox_winner'], dict):
        if 'price' in record['buybox_winner']:
            raw_price = record['buybox_winner']['price']
            price_value = extract_price(raw_price)
            logger.info(f"    Found buybox_winner.price: {raw_price} -> {price_value}")
        else:
            logger.info(f"    buybox_winner found but no price field")
    else:
        logger.info(f"    No buybox_winner field found")

    # Fallback to direct price field
    if price_value is None and 'price' in record:
        raw_price = record['price']
        price_value = extract_price(raw_price)
        logger.info(f"    Found direct price: {raw_price} -> {price_value}")

    # Set the price fields if we found a value
    if price_value:
        standardized["Rate"] = price_value
        standardized["Purchase Rate"] = price_value
        standardized["CF.Cost"] = price_value
        logger.info(f"    Set Rate to: {price_value}")
    else:
        logger.info(f"    No price found for this product")

    # Extract brand/manufacturer
    if 'brand' in record and record['brand']:
        standardized["CF.Manufacturer"] = str(record['brand'])

    # Extract category
    if 'category' in record and record['category']:
        standardized["Item Type"] = str(record['category'])
    elif 'categories' in record and record['categories'] and isinstance(record['categories'], list) and len(record['categories']) > 0:
        standardized["Item Type"] = str(record['categories'][0])

    # Extract URL
    if 'link' in record and record['link']:
        standardized["CF.URL"] = str(record['link'])
    elif 'url' in record and record['url']:
        standardized["CF.URL"] = str(record['url'])

    # Extract material
    if 'specifications' in record and isinstance(record['specifications'], list):
        for spec in record['specifications']:
            if isinstance(spec, dict) and 'key' in spec and 'value' in spec:
                if spec['key'].lower() in ['material', 'materials']:
                    standardized["CF.Material"] = str(spec['value'])

    # Map any remaining fields from the record
    for field, value in record.items():
        # Skip fields we've already processed
        if field in ['identifiers', 'title', 'name', 'description', 'details', 'price', 'brand', 'category', 'categories', 'link', 'url', 'specifications']:
            continue

        # Try to map the field to a standard header
        standard_field = map_field_name(field)

        # Only add if it's a standard header and not already set
        if standard_field in STANDARD_HEADERS and not standardized[standard_field]:
            standardized[standard_field] = str(value)

    return standardized

# Function to generate a unique key for a record to detect duplicates
def generate_record_key(record: Dict[str, Any]) -> str:
    """
    Generate a unique key for a record based on its identifying fields.
    This is used to detect duplicate records.
    """
    # Use Item ID as the primary key if available
    if record.get("Item ID"):
        return f"id:{record['Item ID']}"

    # Use SKU as a fallback
    if record.get("SKU"):
        return f"sku:{record['SKU']}"

    # Use Item Name + Description as a last resort
    if record.get("Item Name") and record.get("Description"):
        name = record["Item Name"].strip().lower()
        desc = record["Description"].strip().lower()
        return f"name_desc:{name}_{desc[:50]}"

    # If no good identifying fields, use a hash of all non-empty values
    values = [str(v) for k, v in record.items() if v and k in STANDARD_HEADERS]
    if values:
        return f"hash:{'_'.join(values[:5])}"

    # Last resort - return a random string (will not detect duplicates)
    return f"unknown:{time.time()}"


# Function to extract price from various formats
def extract_price(value: Any) -> str:
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

# Progress callback type
ProgressCallback = Callable[[int, int, str], None]

def extract_product_data(json_data: Dict[str, Any]) -> Optional[Union[Dict[str, Any], List[Dict[str, Any]]]]:
    """
    Extract product data from Home Depot JSON structure.

    Args:
        json_data: The loaded JSON data

    Returns:
        Dictionary with product data, list of products, or None if not found
    """
    # Check if this is a product JSON
    if 'product' in json_data and isinstance(json_data['product'], dict):
        # Create a merged product object with both product data and pricing
        product_data = json_data['product'].copy()

        # Add top-level buybox_winner data if available (contains pricing)
        if 'buybox_winner' in json_data and isinstance(json_data['buybox_winner'], dict):
            # Directly add the price to the product data for easier access
            if 'price' in json_data['buybox_winner']:
                product_data['price'] = json_data['buybox_winner']['price']

            # Also keep the full buybox_winner data
            product_data['buybox_winner'] = json_data['buybox_winner']

        # Check for nested buybox_winner in product
        elif 'buybox_winner' in product_data and isinstance(product_data['buybox_winner'], dict):
            # Directly add the price to the product data for easier access
            if 'price' in product_data['buybox_winner']:
                product_data['price'] = product_data['buybox_winner']['price']

        return product_data

    # Check if this is a search results JSON with products (array format)
    if 'search_results' in json_data and isinstance(json_data['search_results'], list):
        products = []
        for result in json_data['search_results']:
            if isinstance(result, dict):
                # Create a merged product object
                product_data = {}

                # Add product data if available
                if 'product' in result and isinstance(result['product'], dict):
                    product_data.update(result['product'])

                # Add offers data if available (contains pricing)
                if 'offers' in result and isinstance(result['offers'], dict):
                    product_data['offers'] = result['offers']

                if product_data:  # Only add if we have some data
                    products.append(product_data)

        return products if products else None

    # Check if this is a search results JSON with products (object format)
    if 'search_results' in json_data and isinstance(json_data['search_results'], dict):
        if 'products' in json_data['search_results'] and isinstance(json_data['search_results']['products'], list):
            # Process each product to ensure it has pricing data
            products = []
            for product in json_data['search_results']['products']:
                if isinstance(product, dict):
                    products.append(product)
            return products if products else None

    # Check if this is a direct product list
    if 'products' in json_data and isinstance(json_data['products'], list):
        return json_data['products']

    # No recognized product data structure
    return None

def debug_price_extraction(file_path: str) -> None:
    """
    Debug function to print price information from a JSON file.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            json_data = json.load(f)

            print(f"\nDEBUG - File: {file_path}")

            # Check for buybox_winner price at top level
            if 'buybox_winner' in json_data and isinstance(json_data['buybox_winner'], dict):
                if 'price' in json_data['buybox_winner']:
                    print(f"  Found buybox_winner.price: {json_data['buybox_winner']['price']}")
                else:
                    print("  No price in buybox_winner")
            else:
                print("  No top-level buybox_winner found")

            # Check for buybox_winner inside product
            if 'product' in json_data and isinstance(json_data['product'], dict):
                if 'buybox_winner' in json_data['product'] and isinstance(json_data['product']['buybox_winner'], dict):
                    if 'price' in json_data['product']['buybox_winner']:
                        print(f"  Found product.buybox_winner.price: {json_data['product']['buybox_winner']['price']}")
                    else:
                        print("  No price in product.buybox_winner")
                else:
                    print("  No product.buybox_winner found")

            # Check for product data
            if 'product' in json_data and isinstance(json_data['product'], dict):
                product = json_data['product']
                print(f"  Product title: {product.get('title', 'No title')}")

                # Check for direct price in product
                if 'price' in product:
                    print(f"  Found product.price: {product['price']}")
                else:
                    print("  No direct price in product")

                # Extract and standardize
                product_data = extract_product_data(json_data)
                if product_data and isinstance(product_data, dict):
                    standardized = standardize_record(product_data)
                    print(f"  Standardized Rate: {standardized.get('Rate', 'Not found')}")
                    print(f"  Standardized Item ID: {standardized.get('Item ID', 'Not found')}")
                    print(f"  Standardized Item Name: {standardized.get('Item Name', 'Not found')}")
            else:
                print("  No product data found")

    except Exception as e:
        print(f"  Error debugging file: {str(e)}")

def process_json_file(file_path: str, known_records: Optional[Dict[str, bool]] = None) -> Tuple[List[Dict[str, Any]], int, int, List[str]]:
    """
    Process a single JSON file and extract standardized data, skipping duplicates.
    Handles Home Depot specific JSON structure.

    Args:
        file_path: Path to the JSON file
        known_records: Dictionary of already processed record keys

    Returns:
        Tuple containing:
        - List of standardized data items
        - Number of records processed
        - Number of duplicate records skipped
        - List of errors
    """
    standardized_data = []
    record_count = 0
    duplicate_count = 0
    errors = []

    # Initialize known_records if not provided
    if known_records is None:
        known_records = {}

    # Log file processing
    logger.info(f"Processing file: {os.path.basename(file_path)}")

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            try:
                # Read the file content
                file_content = f.read()

                # Log file size and first 100 characters
                logger.info(f"  File size: {len(file_content)} bytes")
                preview_text = file_content[:100].replace('\n', ' ')
                logger.info(f"  File preview: {preview_text}...")

                # Parse JSON
                json_data = json.loads(file_content)

                # DIRECT APPROACH FOR HOME DEPOT FILES
                # Check if this is a Home Depot product file with the expected structure
                if 'product' in json_data and isinstance(json_data['product'], dict):
                    # Create a product object with the price information
                    product = json_data['product'].copy()

                    # Add buybox_winner if it exists at the top level
                    if 'buybox_winner' in json_data and isinstance(json_data['buybox_winner'], dict):
                        product['buybox_winner'] = json_data['buybox_winner']

                    # Standardize the record
                    try:
                        standardized = standardize_record(product)

                        # Generate a unique key for duplicate detection
                        record_key = generate_record_key(standardized)

                        # Check if this record has already been processed
                        if record_key in known_records:
                            duplicate_count += 1
                        else:
                            # Mark this record as processed
                            known_records[record_key] = True

                            # Add to our data
                            standardized_data.append(standardized)
                            record_count += 1
                    except Exception as e:
                        errors.append(f"Error standardizing record in {file_path}: {str(e)}")

                # FALLBACK APPROACH FOR OTHER FILES
                else:
                    # Extract product data based on file structure
                    product_data = extract_product_data(json_data)

                    if product_data is None:
                        # Fall back to treating the whole JSON as the product data
                        items = [json_data] if isinstance(json_data, dict) else json_data if isinstance(json_data, list) else []
                    elif isinstance(product_data, list):
                        # Multiple products (search results)
                        items = product_data
                    else:
                        # Single product
                        items = [product_data]

                    for item in items:
                        if isinstance(item, dict):
                            try:
                                # Standardize the record
                                standardized = standardize_record(item)

                                # Generate a unique key for duplicate detection
                                record_key = generate_record_key(standardized)

                                # Check if this record has already been processed
                                if record_key in known_records:
                                    duplicate_count += 1
                                    continue  # Skip this duplicate record

                                # Mark this record as processed
                                known_records[record_key] = True

                                # Add to our data
                                standardized_data.append(standardized)
                                record_count += 1
                            except Exception as e:
                                errors.append(f"Error standardizing record in {file_path}: {str(e)}")
                        else:
                            errors.append(f"Skipped non-dictionary item in {file_path}")
            except json.JSONDecodeError as e:
                errors.append(f"Error parsing {file_path}: {str(e)}")
    except Exception as e:
        errors.append(f"Error reading {file_path}: {str(e)}")

    return standardized_data, record_count, duplicate_count, errors

def combine_json_to_csv(input_path: Union[str, List[str]], output_file: str,
                       progress_callback: Optional[ProgressCallback] = None,
                       max_workers: int = 4,
                       batch_size: int = 10000,
                       skip_duplicates: bool = True) -> Dict[str, Any]:
    """
    Combines multiple JSON files into a single CSV file with improved performance for large datasets.
    Standardizes the data according to predefined column headers and can skip duplicate records.

    Args:
        input_path: Path pattern for JSON files (e.g., "data/*.json") or a list of file paths
        output_file: Path for the output CSV file
        progress_callback: Optional callback function to report progress
        max_workers: Maximum number of worker threads for parallel processing
        batch_size: Number of records to write in each batch
        skip_duplicates: Whether to skip duplicate records (default: True)

    Returns:
        Dictionary with statistics about the conversion process
    """
    start_time = time.time()
    stats = {
        "files_processed": 0,
        "files_with_errors": 0,
        "records_processed": 0,
        "duplicates_skipped": 0,
        "errors": [],
        "elapsed_time": 0
    }

    # Get all JSON files - either from a pattern or a list
    if isinstance(input_path, str):
        # Input is a glob pattern
        json_files = glob.glob(input_path)
    else:
        # Input is already a list of files
        json_files = input_path

    total_files = len(json_files)

    if not json_files:
        logger.warning(f"No JSON files found matching pattern: {input_path}")
        stats["errors"].append(f"No JSON files found matching pattern: {input_path}")
        return stats

    logger.info(f"Found {total_files} JSON files to process")
    if progress_callback:
        progress_callback(0, total_files, "Starting file scan")

    all_errors: List[str] = []
    processed_count = 0

    # Dictionary to track unique records (for duplicate detection)
    known_records = {} if skip_duplicates else None

    # Use a thread pool to process files in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all file processing tasks with the known_records dictionary
        future_to_file = {executor.submit(process_json_file, file, known_records): file for file in json_files}

        # Create CSV file and writer with Excel-compatible settings
        with open(output_file, 'w', newline='', encoding='utf-8-sig') as csvfile:
            # Use our standard headers
            writer = csv.DictWriter(csvfile, fieldnames=STANDARD_HEADERS)
            writer.writeheader()

            # Write a blank row to ensure Excel recognizes the headers
            blank_row = {header: '' for header in STANDARD_HEADERS}
            writer.writerow(blank_row)

            # Temporary storage for batch processing
            temp_data = []

            # Process completed tasks
            for i, future in enumerate(as_completed(future_to_file)):
                file = future_to_file[future]
                try:
                    data, record_count, duplicate_count, errors = future.result()

                    # Update statistics
                    processed_count += 1
                    stats["files_processed"] += 1
                    stats["records_processed"] += record_count
                    stats["duplicates_skipped"] += duplicate_count

                    if errors:
                        stats["files_with_errors"] += 1
                        all_errors.extend(errors)

                    # Store data for batch processing
                    temp_data.extend(data)

                    # Report progress with duplicate info
                    if progress_callback:
                        message = f"Processed {file} ({record_count} records, {duplicate_count} duplicates skipped)"
                        progress_callback(processed_count, total_files, message)

                    # Write in batches to avoid memory issues
                    if len(temp_data) >= batch_size:
                        # Write batch
                        for item in temp_data:
                            writer.writerow(item)

                        # Clear temporary data
                        temp_data = []

                except Exception as e:
                    logger.error(f"Error processing {file}: {str(e)}")
                    stats["errors"].append(f"Error processing {file}: {str(e)}")
                    stats["files_with_errors"] += 1

            # Write any remaining data
            if temp_data:
                for item in temp_data:
                    writer.writerow(item)

    # Update statistics
    stats["elapsed_time"] = time.time() - start_time
    stats["errors"].extend(all_errors)

    # Log completion with duplicate information
    logger.info(f"Conversion complete! Processed {stats['files_processed']} files with {stats['records_processed']} records in {stats['elapsed_time']:.2f} seconds")
    if stats['duplicates_skipped'] > 0:
        logger.info(f"Skipped {stats['duplicates_skipped']} duplicate records")

    # Final progress update
    if progress_callback:
        message = f"Conversion complete: {stats['records_processed']} records processed, {stats['duplicates_skipped']} duplicates skipped"
        progress_callback(total_files, total_files, message)

    return stats

if __name__ == "__main__":
    # Example usage
    import argparse

    # Set up command line arguments
    parser = argparse.ArgumentParser(description='Convert JSON files to CSV with standardized headers')
    parser.add_argument('--input', '-i', default='data/*.json', help='Input pattern for JSON files (e.g., "data/*.json")')
    parser.add_argument('--output', '-o', default='output.csv', help='Output CSV file name')
    parser.add_argument('--workers', '-w', type=int, default=4, help='Number of worker threads')
    parser.add_argument('--batch-size', '-b', type=int, default=10000, help='Batch size for processing')
    parser.add_argument('--allow-duplicates', '-d', action='store_true', help='Allow duplicate records (default: skip duplicates)')
    parser.add_argument('--debug', action='store_true', help='Run in debug mode to diagnose price extraction')

    args = parser.parse_args()

    # Debug mode - analyze a few files to diagnose price extraction
    if args.debug:
        print("\nRunning in debug mode to diagnose price extraction...")
        debug_files = glob.glob('uploads/homedepot_raw_product_*.json')[:3]  # First 3 product files
        for file in debug_files:
            debug_price_extraction(file)
        print("\nDebug complete. Check the output above for price extraction details.")
    else:
        # Run the conversion with the specified settings
        stats = combine_json_to_csv(
            input_path=args.input,
            output_file=args.output,
            max_workers=args.workers,
            batch_size=args.batch_size,
            skip_duplicates=not args.allow_duplicates
        )

        # Print summary
        print(f"\nConversion Summary:")
        print(f"Files processed: {stats['files_processed']}")
        print(f"Records processed: {stats['records_processed']}")
        print(f"Duplicates skipped: {stats['duplicates_skipped']}")
        print(f"Files with errors: {stats['files_with_errors']}")
        print(f"Processing time: {stats['elapsed_time']:.2f} seconds")
        print(f"\nCSV file saved as: {args.output}")

        # Print any errors
        if stats['errors']:
            print(f"\nErrors encountered:")
            for i, error in enumerate(stats['errors'][:10], 1):  # Show first 10 errors
                print(f"{i}. {error}")
            if len(stats['errors']) > 10:
                print(f"...and {len(stats['errors']) - 10} more errors.")