from prefect import task
from typing import List, Dict, Any, Optional
import os
import json
import shutil
from prefect import get_run_logger
from pathlib import Path

def clean_filename(filename: str) -> str:
    """
    Clean a filename by removing all extensions and 'gt_' prefix.
    Example: 'gt_TEST_20250505_R7P4_R8P2.csv' -> 'TEST_20250505_R7P4_R8P2'
    
    Args:
        filename: The filename to clean
        
    Returns:
        Cleaned filename
    """
    # Remove all extensions
    base_name = os.path.splitext(filename)[0]  # Remove last extension
    while '.' in base_name:  # Keep removing extensions until none left
        base_name = os.path.splitext(base_name)[0]
    
    # Remove 'gt_' anywhere in the filename
    base_name = base_name.replace('_gt', '')
    
    return base_name

@task(name="match_files", retries=2)
def match_files(
    source_dir: str,
    target_dir: str,
) -> Dict[str, str]:
    """
    Match files between source and target directories.
    
    Args:
        source_dir: Directory containing source files
        target_dir: Directory containing target files to match against
        
    Returns:
        Dictionary mapping source filenames to their matched target filenames
    """
    logger = get_run_logger()
    logger.info(f"Starting file matching between {source_dir} and {target_dir}")
    
    # Get all files from both directories
    logger.info("Getting source files...")
    source_files = {}
    for f in os.listdir(source_dir):
        if f.startswith('.'):
            continue  # Skip hidden files
        full_path = os.path.join(source_dir, f)
        if os.path.isfile(full_path):
            clean_name = clean_filename(f)
            source_files[f] = clean_name
            logger.info(f"Source file: {f} -> {clean_name}")
    logger.info(f"Found {len(source_files)} source files")
    
    logger.info("Getting target files...")
    target_files = {}
    try:
        for f in os.listdir(target_dir):
            if f.startswith('.'):
                continue  # Skip hidden files
            full_path = os.path.join(target_dir, f)
            if os.path.isfile(full_path):
                clean_name = clean_filename(f)
                target_files[f] = clean_name
                logger.info(f"Target file: {f} -> {clean_name}")
    except Exception as e:
        logger.error(f"Error processing target files: {str(e)}")
        raise
    logger.info(f"Found {len(target_files)} target files")
    
    # Create reverse mapping for target files
    target_reverse = {v: k for k, v in target_files.items()}
    
    # Match files
    logger.info("Matching files...")
    matches = {}
    for source_file, clean_source in source_files.items():
        if clean_source in target_reverse:
            matches[source_file] = target_reverse[clean_source]
            logger.info(f"Matched {source_file} -> {target_reverse[clean_source]}")
    
    logger.info(f"Found {len(matches)} matches")
    return matches

@task(name="save_processed_file", retries=2)
def save_processed_file(data: Dict[str, Any], output_path: str) -> str:
    """
    Save processed data to a new location.
    
    Args:
        data: The data to save
        output_path: Where to save the file
        
    Returns:
        Path where the file was saved
    """
    # Ensure the directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    with open(output_path, 'w') as f:
        json.dump(data, f, indent=2)
    
    return output_path

@task(name="copy_to_checkpoint", retries=2)
def copy_to_checkpoint(file_path: str, checkpoint_dir: str) -> str:
    """
    Copy a file to the checkpoint directory.
    
    Args:
        file_path: Path to the file to copy
        checkpoint_dir: Directory to copy the file to
        
    Returns:
        New path of the file in checkpoint directory
    """
    logger = get_run_logger()
    logger.info(f"Copying {file_path} to {checkpoint_dir}")
    
    # Create checkpoint directory if it doesn't exist
    os.makedirs(checkpoint_dir, exist_ok=True)
    
    # Get the filename from the path
    filename = os.path.basename(file_path)
    new_path = os.path.join(checkpoint_dir, filename)
    
    # Copy the file
    shutil.copy2(file_path, new_path)
    logger.info(f"Copied to {new_path}")
    
    return new_path 