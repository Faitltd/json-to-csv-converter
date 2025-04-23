import os
import json
import csv
import threading
import time
import logging
from flask import Flask, render_template, request, redirect, jsonify, send_file
from werkzeug.utils import secure_filename
from json_to_csv_converter import combine_json_to_csv, standardize_record, STANDARD_HEADERS

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['OUTPUT_FOLDER'] = 'outputs'
app.config['MAX_CONTENT_LENGTH'] = 500 * 1024 * 1024  # 500MB max upload size
app.config['MAX_WORKERS'] = 8  # Number of parallel workers
app.config['BATCH_SIZE'] = 10000  # Records per batch

# Create necessary directories if they don't exist
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(app.config['OUTPUT_FOLDER'], exist_ok=True)

# Global dictionary to store conversion tasks and their progress
conversion_tasks = {}

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/test-download')
def test_download():
    """Download a test CSV file with all headers"""
    test_file = os.path.join(app.config['OUTPUT_FOLDER'], 'test_excel.csv')

    # Use Flask's send_file function with Excel-compatible settings
    response = send_file(
        test_file,
        mimetype='text/csv',
        as_attachment=True,
        download_name='test_excel.csv'
    )

    # Add headers to ensure Excel opens the file correctly
    response.headers['Content-Type'] = 'text/csv; charset=utf-8-sig'
    response.headers['Content-Disposition'] = 'attachment; filename="test_excel.csv"'

    return response

@app.route('/convert', methods=['POST'])
def convert():
    # Check if any files were uploaded
    if 'json_files' not in request.files:
        return redirect(request.url)

    files = request.files.getlist('json_files')

    # Check if any files were selected
    if not files or files[0].filename == '':
        return render_template('index.html', error="No files selected")

    # Get output filename
    output_filename = request.form.get('output_filename', 'output')

    # Remove any file extension if present and ensure it's not empty
    base_name = os.path.splitext(output_filename)[0]
    if not base_name.strip():
        base_name = 'output'

    # Add .csv extension
    output_filename = f"{base_name}.csv"

    # Generate a unique task ID
    task_id = str(int(time.time()))

    # Initialize task status
    conversion_tasks[task_id] = {
        'status': 'uploading',
        'progress': 0,
        'total': len(files),
        'message': 'Uploading files...',
        'output_file': output_filename,
        'stats': None,
        'error': None
    }

    # Save uploaded files
    saved_files = []
    for i, file in enumerate(files):
        if file and file.filename.endswith('.json'):
            try:
                # Read the file content
                file_content = file.read()

                # Log file info
                logger.info(f"Uploaded file: {file.filename}, size: {len(file_content)} bytes")
                preview_text = file_content[:100].decode('utf-8', errors='ignore')
                preview_text = preview_text.replace('\n', ' ')
                logger.info(f"Preview: {preview_text}...")

                # Save the file
                filename = secure_filename(file.filename)
                filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)

                # Write the content to the file
                with open(filepath, 'wb') as f:
                    f.write(file_content)

                saved_files.append(filepath)

                # Update progress
                conversion_tasks[task_id]['progress'] = i + 1
                conversion_tasks[task_id]['message'] = f"Uploaded {i+1} of {len(files)} files"
            except Exception as e:
                logger.error(f"Error saving file {file.filename}: {str(e)}")

    if not saved_files:
        conversion_tasks[task_id]['status'] = 'error'
        conversion_tasks[task_id]['error'] = "No valid JSON files uploaded"
        return jsonify({
            'status': 'error',
            'message': "No valid JSON files uploaded",
            'task_id': task_id
        })

    # Create output path
    output_path = os.path.join(app.config['OUTPUT_FOLDER'], output_filename)

    # Update task status
    conversion_tasks[task_id]['status'] = 'processing'
    conversion_tasks[task_id]['progress'] = 0
    conversion_tasks[task_id]['total'] = len(saved_files)
    conversion_tasks[task_id]['message'] = 'Starting conversion...'

    # Skip duplicates setting is no longer used with direct processing

    # Start conversion in a separate thread
    def run_conversion():
        try:
            # Log the files being processed
            logger.info(f"Processing {len(saved_files)} uploaded files:")
            for i, file_path in enumerate(saved_files[:5]):
                logger.info(f"  {i+1}. {os.path.basename(file_path)}")
            if len(saved_files) > 5:
                logger.info(f"  ... and {len(saved_files) - 5} more files")

            # Direct processing for all files
            # Note: We're already importing standardize_record and STANDARD_HEADERS at the top of the file

            # Create CSV file
            with open(output_path, 'w', newline='', encoding='utf-8-sig') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=STANDARD_HEADERS)
                writer.writeheader()

                # Process each file directly
                records_processed = 0
                files_processed = 0
                files_with_errors = 0
                errors = []

                for file_path in saved_files:
                    try:
                        logger.info(f"Processing file: {os.path.basename(file_path)}")

                        # Load the file
                        with open(file_path, 'r', encoding='utf-8') as f:
                            json_data = json.load(f)

                        # Check if this is a product file
                        if 'product' in json_data and isinstance(json_data['product'], dict):
                            product = json_data['product']

                            # Check for buybox_winner at top level
                            if 'buybox_winner' in json_data and isinstance(json_data['buybox_winner'], dict):
                                logger.info("Found buybox_winner at top level")
                                if 'price' in json_data['buybox_winner']:
                                    logger.info(f"Found price: {json_data['buybox_winner']['price']}")
                                    # Add the price to the product for standardization
                                    product['buybox_winner'] = json_data['buybox_winner']

                            # Check for description
                            if 'description' in product and product['description']:
                                logger.info(f"Found description in product: {product['description'][:50]}...")
                            else:
                                logger.warning("No description found in product")
                                # Try to find description in other places
                                if 'description_full' in product:
                                    logger.info("Using description_full instead")
                                    product['description'] = product['description_full']
                                elif 'long_description' in product:
                                    logger.info("Using long_description instead")
                                    product['description'] = product['long_description']
                                elif 'details' in product:
                                    logger.info("Using details instead")
                                    product['description'] = product['details']
                                # Use title as description if no description is found
                                elif 'title' in product:
                                    logger.info(f"Using title as description: {product['title']}")
                                    product['description'] = product['title']

                            # Standardize the record
                            standardized = standardize_record(product)

                        # Check if this is a search results file
                        elif 'search_results' in json_data:
                            logger.info("Found search_results structure")
                            search_results = json_data['search_results']

                            # Handle different search results structures
                            products = []

                            # Check for products array in search_results
                            if isinstance(search_results, dict) and 'products' in search_results and isinstance(search_results['products'], list):
                                logger.info(f"Found {len(search_results['products'])} products in search_results.products")
                                products = search_results['products']
                            # Check if search_results is itself an array of products
                            elif isinstance(search_results, list):
                                logger.info(f"Found {len(search_results)} products in search_results array")
                                products = search_results

                            # Process each product
                            if products:
                                for product in products:
                                    if isinstance(product, dict):
                                        # Check if this is a product with data
                                        if 'product' in product and isinstance(product['product'], dict):
                                            product_data = product['product']

                                            # Check for price in offers
                                            if 'offers' in product and isinstance(product['offers'], dict):
                                                if 'primary' in product['offers'] and isinstance(product['offers']['primary'], dict):
                                                    if 'price' in product['offers']['primary']:
                                                        price = product['offers']['primary']['price']
                                                        logger.info(f"Found price in offers.primary: {price}")
                                                        product_data['price'] = price

                                            # Check for description
                                            if 'description' in product and product['description']:
                                                logger.info(f"Found description in product: {product['description'][:50]}...")
                                                product_data['description'] = product['description']

                                            # Check for snippet
                                            if 'snippet' in product and product['snippet']:
                                                logger.info(f"Found snippet in product: {product['snippet'][:50]}...")
                                                if 'description' not in product_data or not product_data['description']:
                                                    product_data['description'] = product['snippet']

                                            # Check for content_spec
                                            if 'content_spec' in product and isinstance(product['content_spec'], dict):
                                                if 'description' in product['content_spec'] and product['content_spec']['description']:
                                                    logger.info(f"Found description in content_spec: {product['content_spec']['description'][:50]}...")
                                                    if 'description' not in product_data or not product_data['description']:
                                                        product_data['description'] = product['content_spec']['description']

                                            # Check for specifications
                                            if 'specifications' in product and isinstance(product['specifications'], list):
                                                for spec in product['specifications']:
                                                    if isinstance(spec, dict) and 'key' in spec and 'value' in spec:
                                                        if spec['key'] == 'Description' and spec['value']:
                                                            logger.info(f"Found description in specifications: {spec['value'][:50]}...")
                                                            if 'description' not in product_data or not product_data['description']:
                                                                product_data['description'] = spec['value']

                                            # Use title as description if no description is found
                                            if ('description' not in product_data or not product_data['description']) and 'title' in product_data:
                                                logger.info(f"Using title as description: {product_data['title']}")
                                                product_data['description'] = product_data['title']

                                            # Standardize the record
                                            standardized = standardize_record(product_data)

                                            # Log the standardized record
                                            logger.info(f"Standardized record from search results: {standardized}")

                                            # Check if the record has any data
                                            has_data = False
                                            for key, value in standardized.items():
                                                if value and key not in ['Source', 'Purchase Account', 'Vendor', 'CF.Markup', 'CF.Supplier']:
                                                    has_data = True
                                                    break

                                            if has_data:
                                                # Write to CSV
                                                writer.writerow(standardized)
                                                records_processed += 1
                                                logger.info(f"Record written to CSV from search results")

                                                # Log the result
                                                if standardized.get('Rate'):
                                                    logger.info(f"Extracted price: {standardized['Rate']}")
                                                else:
                                                    logger.warning(f"No price extracted for product in search results")
                                            else:
                                                logger.warning(f"Record from search results has no meaningful data, skipping")
                                continue  # Skip the rest of the processing for this file since we've handled the search results

                        # For product files, continue with the existing logic
                        if 'product' in json_data and isinstance(json_data['product'], dict):
                            # Log the standardized record
                            logger.info(f"Standardized record: {standardized}")

                            # Check if the record has any data
                            has_data = False
                            for key, value in standardized.items():
                                if value and key not in ['Source', 'Purchase Account', 'Vendor', 'CF.Markup', 'CF.Supplier']:
                                    has_data = True
                                    break

                            if has_data:
                                # Write to CSV
                                writer.writerow(standardized)
                                records_processed += 1
                                logger.info(f"Record written to CSV")

                                # Log the result
                                if standardized.get('Rate'):
                                    logger.info(f"Extracted price: {standardized['Rate']}")
                                else:
                                    logger.warning(f"No price extracted for {os.path.basename(file_path)}")
                            else:
                                logger.warning(f"Record has no meaningful data, skipping: {os.path.basename(file_path)}")
                        else:
                            logger.warning(f"No product data found in {os.path.basename(file_path)}")
                            errors.append(f"No product data found in {os.path.basename(file_path)}")
                            files_with_errors += 1

                        files_processed += 1
                    except Exception as e:
                        logger.error(f"Error processing {os.path.basename(file_path)}: {str(e)}")
                        errors.append(f"Error processing {os.path.basename(file_path)}: {str(e)}")
                        files_with_errors += 1

                # Create stats
                stats = {
                    "files_processed": files_processed,
                    "files_with_errors": files_with_errors,
                    "records_processed": records_processed,
                    "duplicates_skipped": 0,
                    "errors": errors,
                    "elapsed_time": 0
                }

                logger.info(f"Direct processing complete. CSV file created: {output_path}")
                logger.info(f"Processed {files_processed} files, {records_processed} records")

            # Log the results
            logger.info(f"Conversion results: {stats['records_processed']} records processed, {stats['duplicates_skipped']} duplicates skipped")

            # Update task status
            conversion_tasks[task_id]['status'] = 'completed'
            conversion_tasks[task_id]['message'] = 'Conversion complete'
            conversion_tasks[task_id]['stats'] = stats

            # Clean up old tasks after 1 hour
            def cleanup():
                time.sleep(3600)  # 1 hour
                if task_id in conversion_tasks:
                    del conversion_tasks[task_id]

            cleanup_thread = threading.Thread(target=cleanup)
            cleanup_thread.daemon = True
            cleanup_thread.start()

        except Exception as e:
            logger.error(f"Error during conversion: {str(e)}")
            conversion_tasks[task_id]['status'] = 'error'
            conversion_tasks[task_id]['error'] = str(e)
            conversion_tasks[task_id]['message'] = f"Error: {str(e)}"

    # Start the conversion thread
    conversion_thread = threading.Thread(target=run_conversion)
    conversion_thread.daemon = True
    conversion_thread.start()

    # Return task ID for status polling
    return jsonify({
        'status': 'processing',
        'message': 'Conversion started',
        'task_id': task_id
    })

@app.route('/status/<task_id>', methods=['GET'])
def get_status(task_id):
    """Get the status of a conversion task"""
    if task_id not in conversion_tasks:
        return jsonify({
            'status': 'error',
            'message': 'Task not found'
        }), 404

    return jsonify(conversion_tasks[task_id])

@app.route('/download/<task_id>', methods=['GET'])
def download_file(task_id):
    """Download the converted CSV file"""
    if task_id not in conversion_tasks:
        return jsonify({
            'status': 'error',
            'message': 'Task not found'
        }), 404

    task = conversion_tasks[task_id]

    if task['status'] != 'completed':
        return jsonify({
            'status': 'error',
            'message': 'Conversion not yet complete'
        }), 400

    output_path = os.path.join(app.config['OUTPUT_FOLDER'], task['output_file'])

    try:
        # Use Flask's send_file function with Excel-compatible settings
        # This ensures proper handling of the file content and headers
        response = send_file(
            output_path,
            mimetype='text/csv',
            as_attachment=True,
            download_name=task['output_file']
        )

        # Add headers to ensure Excel opens the file correctly
        response.headers['Content-Type'] = 'text/csv; charset=utf-8-sig'
        response.headers['Content-Disposition'] = f'attachment; filename="{task["output_file"]}"'

        return response
    except Exception as e:
        logger.error(f"Error reading output file: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': f"Error reading output file: {str(e)}"
        }), 500

if __name__ == '__main__':
    # This is used when running locally only. When deploying to Cloud Run,
    # a webserver process such as Gunicorn will serve the app.
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
