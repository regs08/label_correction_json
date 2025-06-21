#!/usr/bin/env python3
"""
Azure Upload Script

This script uploads files to Azure blob storage but skips files that already exist.
Uses the existing azure_tasks.py functions.

Usage:
    # Configure paths and settings at the top of the script, then run:
    python upload_to_azure_script.py
"""

import sys
from pathlib import Path
import logging
from typing import List

from azure.core.exceptions import ResourceExistsError
from src.tasks.azure_tasks import list_label_files, upload_label_file
from src.utils.config import get_settings

# =============================================================================
# CONFIGURATION - Modify these settings as needed
# =============================================================================

# Input folder containing files to upload
INPUT_FOLDER = Path("output")

# File pattern to match (e.g., "*.json", "*.pdf", "*" for all files)
FILE_PATTERN = "*.json"

# Optional prefix to add to blob names in Azure
BLOB_PREFIX = None  # Set to something like "processed/" if needed

# Azure container to upload to (from settings)
# This will use the destination container from your config

# =============================================================================

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('azure_upload.log', mode='w'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def validate_inputs(input_folder: Path) -> bool:
    """
    Validate input folder exists and is accessible.
    
    Args:
        input_folder: Path to folder containing files
        
    Returns:
        bool: True if inputs are valid
    """
    if not input_folder.exists():
        logger.error(f"Input folder does not exist: {input_folder}")
        return False
        
    if not input_folder.is_dir():
        logger.error(f"Input path is not a directory: {input_folder}")
        return False
        
    return True


def get_files_to_upload(input_folder: Path, file_pattern: str) -> List[Path]:
    """
    Get list of files matching the pattern in the input folder.
    
    Args:
        input_folder: Path to folder containing files
        file_pattern: Pattern to match files (e.g., "*.json")
        
    Returns:
        List of Path objects for matching files
    """
    files = list(input_folder.glob(file_pattern))
    if not files:
        logger.warning(f"No files found matching pattern '{file_pattern}' in {input_folder}")
    return files


def check_file_exists_in_azure(blob_name: str, existing_blobs: List[str]) -> bool:
    """
    Check if a file already exists in Azure.
    
    Args:
        blob_name: Name of the blob to check
        existing_blobs: List of existing blob names in Azure
        
    Returns:
        bool: True if file exists, False otherwise
    """
    return blob_name in existing_blobs


def upload_file_with_skip(file_path: Path, existing_blobs: List[str], blob_prefix: str = None) -> dict:
    """
    Upload a single file to Azure, skipping if it already exists.
    
    Args:
        file_path: Path to the file to upload
        existing_blobs: List of existing blob names in Azure
        blob_prefix: Optional prefix to add to blob name
        
    Returns:
        dict: Result of the upload operation
    """
    # Create blob name
    blob_name = file_path.name
    if blob_prefix:
        blob_name = f"{blob_prefix}/{blob_name}"
    
    # Check if file already exists
    if check_file_exists_in_azure(blob_name, existing_blobs):
        return {
            "status": "skipped",
            "file": str(file_path),
            "blob_name": blob_name,
            "message": f"File already exists in Azure: {blob_name}"
        }
    
    # Upload the file with overwrite=False
    try:
        uploaded_blob = upload_label_file(str(file_path), blob_name, overwrite=False)
        return {
            "status": "success",
            "file": str(file_path),
            "blob_name": uploaded_blob,
            "message": f"Successfully uploaded: {file_path.name} -> {uploaded_blob}"
        }
    except ResourceExistsError:
        return {
            "status": "skipped",
            "file": str(file_path),
            "blob_name": blob_name,
            "message": f"File already exists in Azure (skipped): {blob_name}"
        }
    except Exception as e:
        return {
            "status": "error",
            "file": str(file_path),
            "blob_name": blob_name,
            "error": f"Failed to upload: {str(e)}"
        }


def main():
    """Main function to run the Azure upload process."""
    print("Starting Azure Upload Script")
    print("=" * 50)
    
    # Get settings
    try:
        settings = get_settings()
        print(f"Azure destination container: {settings.azure_destination_container_name}")
    except Exception as e:
        print(f"Failed to get settings: {str(e)}")
        sys.exit(1)
    
    # Validate inputs
    if not validate_inputs(INPUT_FOLDER):
        sys.exit(1)
    
    # Get files to upload
    files = get_files_to_upload(INPUT_FOLDER, FILE_PATTERN)
    if not files:
        print("No files to process")
        sys.exit(1)
    
    # Get existing blobs in Azure
    print("Checking existing files in Azure...")
    try:
        existing_blobs = list_label_files(BLOB_PREFIX)
        print(f"Found {len(existing_blobs)} existing files in Azure")
        print(f"Existing files: {existing_blobs[:5]}...")  # Show first 5
    except Exception as e:
        print(f"Failed to list existing blobs: {str(e)}")
        sys.exit(1)
    
    print(f"Found {len(files)} file(s) to process")
    print(f"Input folder: {INPUT_FOLDER}")
    print(f"File pattern: {FILE_PATTERN}")
    print(f"Blob prefix: {BLOB_PREFIX or 'None'}")
    print("=" * 50)
    
    # Process files
    results = []
    successful = 0
    failed = 0
    skipped = 0
    
    for i, file_path in enumerate(files, 1):
        print(f"Processing file {i}/{len(files)}: {file_path.name}")
        
        result = upload_file_with_skip(file_path, existing_blobs, BLOB_PREFIX)
        results.append(result)
        
        if result['status'] == 'success':
            successful += 1
            print(f"✅ {result['message']}")
        elif result['status'] == 'skipped':
            skipped += 1
            print(f"⏭️  {result['message']}")
        else:  # error
            failed += 1
            print(f"❌ {result['error']}")
    
    # Summary
    print("=" * 50)
    print("UPLOAD SUMMARY")
    print("=" * 50)
    print(f"Total files processed: {len(files)}")
    print(f"Successfully uploaded: {successful}")
    print(f"Skipped (already exist): {skipped}")
    print(f"Failed: {failed}")
    
    if failed > 0:
        print(f"{failed} file(s) failed to upload. Check the log for details.")
        sys.exit(1)
    else:
        print("All files processed successfully!")


if __name__ == "__main__":
    main() 