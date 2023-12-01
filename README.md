# Baby-NI-BE

## Features
File Monitoring: The application uses FileSystemWatcher to monitor a specified directory for file changes, including creation, modification, deletion, and renaming.

File Processing: Detected files are processed, converted to CSV format, and modified based on their content. The processing includes renaming, aggregating data, and adding additional columns.

Data Aggregation: The processed data is aggregated using the Aggregator class, which utilizes a configuration file to determine aggregation rules. The aggregated data is then inserted into Vertica tables.

Vertica Database Integration: The application connects to a Vertica database. It loads processed CSV files into Vertica tables and performs data aggregation within the database.

NIGetData API Integration: The project includes an API (NIGetData) built with ASP.NET Core. The API facilitates retrieving data from the Vertica database based on specified parameters. It exposes an endpoint to receive HTTP POST requests with a payload containing details such as the network element, table name, and time range. The API connects to a Vertica database, executes a SQL query, and returns the queried data in a structured format.

## Usage
Run the Application: Execute the BabyNiProject application on VSCode. The program will start monitoring the specified directory for file changes.

Monitor and Process Files: As files are created or modified in the monitored directory, the application will process them, convert to CSV, and perform specified modifications.

Load Data to Vertica: Processed CSV files are loaded into a Vertica database. The database connection details are specified in appsettings.json.

Aggregate Data: Aggregated data is generated based on the configured rules and written to the destination tables in the Vertica database.

DataCollector API Usage: The NIGetData API allows external applications to retrieve specific data from the Vertica database. It can be accessed by sending HTTP POST requests to the `/api/GetData/get-data` endpoint with a JSON payload containing details like the network element, table name, and time range.

NIGetData API Details:
- Endpoint: `/api/GetData/get-data`
- Method: HTTP POST
- Payload: JSON object with the following properties:
  - `Ne` (string): Network Element name
  - `table` (string): Table name in the Vertica database
  - `timeStart` (string): Start time for the data query
  - `timeEnd` (string): End time for the data query

The API will execute a SQL query on the Vertica database to retrieve relevant data and respond with a JSON array containing the queried data.
