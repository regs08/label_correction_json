#!/usr/bin/env python3
"""
File Renaming Script

This script renames files in a folder by replacing specified strings in filenames.
Supports both single file and folder processing.

Usage:
    # Configure paths and strings at the top of the script, then run:
    python rename_files_script.py
"""

import sys
from pathlib import Path
import logging
import shutil

# =============================================================================
# CONFIGURATION - Modify these settings as needed
# =============================================================================

# Input folder containing files to rename
INPUT_FOLDER = Path("vgb_training_data/gt")

# File pattern to match (e.g., "*.json", "*.pdf", "*" for all files)
FILE_PATTERN = "*.csv"

# Find and replace strings
FIND_STRING = "VGB"  # String to find in filenames
REPLACE_WITH = "vgb"  # String to replace it with

# Case sensitive matching (True/False)
CASE_SENSITIVE = False

# =============================================================================

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('file_rename.log', mode='w'),
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


def get_files_to_rename(input_folder: Path, file_pattern: str) -> list:
    """
    Get list of files matching the pattern in the input folder.
    
    Args:
        input_folder: Path to folder containing files
        file_pattern: Pattern to match files (e.g., "*.json")
        
    Returns:
        list: List of Path objects for matching files
    """
    files = list(input_folder.glob(file_pattern))
    if not files:
        logger.warning(f"No files found matching pattern '{file_pattern}' in {input_folder}")
    return files


def rename_file(file_path: Path, find_string: str, replace_with: str, case_sensitive: bool = False) -> dict:
    """
    Rename a single file by replacing the specified string.
    
    Args:
        file_path: Path to the file to rename
        find_string: String to find in filename
        replace_with: String to replace it with
        case_sensitive: Whether to use case-sensitive matching
        
    Returns:
        dict: Result of the rename operation
    """
    original_name = file_path.name
    new_name = original_name
    
    # Perform the replacement
    if case_sensitive:
        if find_string in original_name:
            new_name = original_name.replace(find_string, replace_with)
    else:
        # Case-insensitive replacement
        if find_string.lower() in original_name.lower():
            # Find the actual case in the original name
            lower_original = original_name.lower()
            lower_find = find_string.lower()
            start_idx = lower_original.find(lower_find)
            if start_idx != -1:
                end_idx = start_idx + len(lower_find)
                new_name = original_name[:start_idx] + replace_with + original_name[end_idx:]
    
    # Check if the name actually changed
    if new_name == original_name:
        return {
            "status": "no_change",
            "file": str(file_path),
            "message": f"No changes needed for {original_name}"
        }
    
    # Create new path
    new_path = file_path.parent / new_name
    
    # Check if target file already exists
    if new_path.exists():
        return {
            "status": "error",
            "file": str(file_path),
            "error": f"Target file already exists: {new_path.name}"
        }
    
    # Perform the rename
    try:
        file_path.rename(new_path)
        return {
            "status": "success",
            "file": str(file_path),
            "new_name": new_name,
            "message": f"Renamed: {original_name} -> {new_name}"
        }
    except Exception as e:
        return {
            "status": "error",
            "file": str(file_path),
            "error": f"Failed to rename: {str(e)}"
        }


def main():
    """Main function to run the file renaming process."""
    logger.info("Starting File Rename Script")
    logger.info("=" * 50)
    
    # Validate inputs
    if not validate_inputs(INPUT_FOLDER):
        sys.exit(1)
    
    # Get files to rename
    files = get_files_to_rename(INPUT_FOLDER, FILE_PATTERN)
    if not files:
        logger.error("No files to process")
        sys.exit(1)
    
    logger.info(f"Found {len(files)} file(s) to process")
    logger.info(f"Input folder: {INPUT_FOLDER}")
    logger.info(f"File pattern: {FILE_PATTERN}")
    logger.info(f"Find string: '{FIND_STRING}'")
    logger.info(f"Replace with: '{REPLACE_WITH}'")
    logger.info(f"Case sensitive: {CASE_SENSITIVE}")
    logger.info("=" * 50)
    
    # Process files
    results = []
    successful = 0
    failed = 0
    no_change = 0
    
    for i, file_path in enumerate(files, 1):
        logger.info(f"Processing file {i}/{len(files)}: {file_path.name}")
        
        result = rename_file(file_path, FIND_STRING, REPLACE_WITH, CASE_SENSITIVE)
        results.append(result)
        
        if result['status'] == 'success':
            successful += 1
            logger.info(f"✅ {result['message']}")
        elif result['status'] == 'error':
            failed += 1
            logger.error(f"❌ {result['error']}")
        else:  # no_change
            no_change += 1
            logger.info(f"ℹ️  {result['message']}")
    
    # Summary
    logger.info("=" * 50)
    logger.info("RENAMING SUMMARY")
    logger.info("=" * 50)
    logger.info(f"Total files processed: {len(files)}")
    logger.info(f"Successfully renamed: {successful}")
    logger.info(f"No changes needed: {no_change}")
    logger.info(f"Failed: {failed}")
    
    if failed > 0:
        logger.warning(f"{failed} file(s) failed to rename. Check the log for details.")
        sys.exit(1)
    else:
        logger.info("All files processed successfully!")


if __name__ == "__main__":
    main() 