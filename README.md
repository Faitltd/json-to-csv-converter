# JSON to CSV Converter

A web application that converts JSON files to CSV format, specifically designed for processing Home Depot product data.

## Features

- Upload multiple JSON files at once
- Standardize column headers across different JSON structures
- Extract product information including Item ID, Item Name, SKU, Description, and Rate
- Remove duplicate records
- Download the resulting CSV file
- Process 100+ files efficiently

## Usage

1. Upload one or more JSON files using the web interface
2. Click "Convert to CSV"
3. Wait for the processing to complete
4. Download the resulting CSV file

## Installation

### Local Development

1. Clone this repository
2. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```
3. Run the application:
   ```
   python app.py
   ```
4. Open your browser and navigate to http://127.0.0.1:5000/

### Docker Deployment

1. Build the Docker image:
   ```
   docker build -t json-to-csv-converter .
   ```
2. Run the Docker container:
   ```
   docker run -p 8080:8080 json-to-csv-converter
   ```
3. Open your browser and navigate to http://localhost:8080/

## Deployment to Google Cloud Run

1. Build the Docker image:
   ```
   gcloud builds submit --tag gcr.io/YOUR_PROJECT_ID/json-to-csv-converter
   ```
2. Deploy to Cloud Run:
   ```
   gcloud run deploy json-to-csv-converter --image gcr.io/YOUR_PROJECT_ID/json-to-csv-converter --platform managed
   ```

## License

MIT
