# Unit of Work

## Overview
Unit of Work is an ETL (Extract, Transform, Load) Python package for payload migration processing. It extracts by waiting for and confirming the existence of an imported tape, transforms by slicing the tape, performing sanity checks, and creating a future directory structure via symbolic links, and loads by uploading the processed data to S3.

## Features
- **Extraction**: Waits for and confirms the existence of an imported tape.
- **Transformation**: Slices the tape, performs sanity checks, and creates a directory structure via symbolic links.
- **Loading**: Uploads the processed data to S3.

## Prerequisites
Before installing and using Unit of Work, ensure the following prerequisites are met:
- **DB2 Installed and Accessible**: The IBM DB2 database must be installed and properly configured on your system, with the necessary permissions and connectivity for the `ibm_db` Python package to interact with it.
- **AWS S3 CLI Configured Locally with Credentials**: The AWS Command Line Interface (CLI) must be installed and configured on your system with valid AWS credentials (e.g., via `aws configure`) to enable uploading data to S3. Ensure the credentials have appropriate permissions for the S3 bucket specified in your configuration.

## Installation
### Installing Required Dependencies
To install the required dependencies, run:
```bash
pip install -r requirements.txt
```

### Installing Locally
To install the `unit_of_work` package locally from the source code (e.g., after cloning the repository), navigate to the root directory of the project (where `setup.py` is located) and run:
```bash
pip install . --no-index --no-build-isolation
ln -s /ars/home/lib/py3/bin/unit-of-work /ars/home/bin/unit-of-work
ls -l /ars/home/bin 
```
This will install the package and make it available for use in your Python environment.

## Usage
### Running the Installed Program
Once installed, you can run the `unit_of_work` program from the command line using the provided entry point. Use the following command to process a tape:
```bash
unit-of-work --tape-name <TAPE_NAME>
```
Alternatively, if you prefer to run it as a module, you can use:
```bash
python -m unit_of_work --tape-name <TAPE_NAME>
```

## Configuration
The configuration file `payload_migration_config.yaml` should be placed in the `resources` directory. It contains all necessary settings for the ETL process.
