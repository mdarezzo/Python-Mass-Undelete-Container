# Azure Data Lake Storage Mass Undelete Tool

A Python script for efficiently restoring multiple deleted files from Azure Data Lake Storage Gen2. The tool features dynamic concurrency adjustment, progress tracking, and supports both access key and Azure RBAC authentication methods.

## Features

- Mass restoration of deleted files from ADLS Gen2 containers
- Dynamic concurrency adjustment based on server response
- Real-time progress tracking with estimated completion time
- Support for both access key and Azure RBAC authentication
- Automatic error handling and retry mechanisms
- Performance metrics and operation statistics

## Prerequisites

- Python 3.7 or higher
- Azure Storage Account with Data Lake Storage Gen2 enabled
- Either:
  - Storage Account access key, OR
  - Azure RBAC permissions (Storage Blob Data Contributor role or equivalent)

## Installation

1. Clone or download this repository
2. Create a virtual environment:
   ```bash
   python -m venv venv
   ```

3. Activate the virtual environment:
   - Windows:
     ```bash
     venv\Scripts\activate
     ```
   - Linux/MacOS:
     ```bash
     source venv/bin/activate
     ```

4. Install required packages:
   ```bash
   pip install -r requirements.txt
   ```

## Authentication

The script supports two authentication methods:

1. **Access Key Authentication**
   - Use when you have the storage account's access key
   - Provide the access key using the `-k` or `--access-key` parameter

2. **Azure RBAC Authentication (DefaultAzureCredential)**
   - Uses your Azure CLI, Visual Studio, or environment credentials
   - No additional parameters needed
   - Requires appropriate RBAC permissions on the storage account
   - Supports managed identities when running in Azure

## Usage

Basic command structure:
```bash
python mass_undelete.py -u <storage-uri> -c <container-name> [-k <access-key>]
```

### Parameters

- `-u, --storage-uri`: Storage account URI (required)
  - Format: `https://<account-name>.dfs.core.windows.net`
- `-c, --container`: Container name to restore deleted items from (required)
- `-k, --access-key`: Storage account access key (optional)
  - If not provided, DefaultAzureCredential will be used

### Examples

1. Using access key authentication:
```bash
python mass_undelete.py -u https://mystorageaccount.dfs.core.windows.net -c mycontainer -k "your-access-key"
```

2. Using Azure RBAC authentication:
```bash
python mass_undelete.py -u https://mystorageaccount.dfs.core.windows.net -c mycontainer
```

## Output

The script provides real-time progress information including:
- Number of items restored/failed
- Current processing speed (items/second)
- Estimated time remaining
- Error rate
- Current concurrency level
- Total elapsed time

## Error Handling

- Automatically adjusts concurrency based on server response
- Handles common errors like throttling and server busy conditions
- Provides detailed error messages for failed restorations
- Tracks and displays error rates in real-time

## Performance

The tool includes several performance optimization features:
- Dynamic concurrency adjustment (10-600 concurrent operations)
- Batch processing of items
- Prioritized restoration based on path depth
- Automatic throttling management

## Notes

- The script will restore all deleted items in the specified container
- Items are restored in order of path depth (shorter paths first)
- Progress is maintained even if some items fail to restore
- "BlobAlreadyExists" errors are ignored as they indicate the item was already restored
