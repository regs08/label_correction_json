#!/usr/bin/env python3
"""
Label Population Script

This script populates JSON label templates with values from CSV ground truth files.
Supports both single file and batch folder processing.

Usage:
    # Configure paths at the top of the script, then run:
    python label_population_script.py
"""

import sys
from pathlib import Path
from typing import List
import logging
from src.flows.label_population_flow import label_population_flow
from src.utils.config import Settings

# =============================================================================
# CONFIGURATION - Modify these paths as needed
# =============================================================================

# Choose one of these options:
#CSV_FILE = Path("vgb_training_data/gt/VGB_061225_3501_3505.csv")  # Single file
CSV_FOLDER = Path("vgb_training_data/gt/")  # Folder of CSV files

# Template file
TEMPLATE_FILE = Path("templates/adjusted/tempalte_last.pdf.labels.json")

# Output directory
OUTPUT_DIR = Path("output")

# =============================================================================

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('label_population.log', mode='w'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def validate_inputs(csv_path: Path, template_path: Path) -> bool:
    """
    Validate input files exist and are accessible.
    
    Args:
        csv_path: Path to CSV file or folder
        template_path: Path to JSON template
        
    Returns:
        bool: True if all inputs are valid
    """
    if not csv_path.exists():
        logger.error(f"CSV path does not exist: {csv_path}")
        return False
        
    if not template_path.exists():
        logger.error(f"Template path does not exist: {template_path}")
        return False
        
    if not template_path.suffix == '.json':
        logger.error(f"Template must be a JSON file: {template_path}")
        return False
        
    return True


def get_csv_files(csv_path: Path) -> List[Path]:
    """
    Get list of CSV files from path (file or folder).
    
    Args:
        csv_path: Path to CSV file or folder
        
    Returns:
        List[Path]: List of CSV file paths
    """
    if csv_path.is_file():
        return [csv_path]
    elif csv_path.is_dir():
        csv_files = list(csv_path.glob("*.csv"))
        if not csv_files:
            logger.warning(f"No CSV files found in directory: {csv_path}")
        return csv_files
    else:
        logger.error(f"Invalid CSV path: {csv_path}")
        return []


def generate_output_path(csv_file: Path, template_path: Path, output_dir: Path) -> Path:
    """
    Generate output path for processed file.
    
    Args:
        csv_file: Input CSV file path
        template_path: Template JSON file path
        output_dir: Output directory
        
    Returns:
        Path: Output file path
    """
    # Create output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate filename based on CSV name
    csv_stem = csv_file.stem
    # Remove common suffixes like '_gt', '_ground_truth'
    for suffix in ['_gt', '_ground_truth', '_groundtruth']:
        if csv_stem.endswith(suffix):
            csv_stem = csv_stem[:-len(suffix)]
            break
    
    output_filename = f"{csv_stem}.pdf.labels.json".lower()
    return output_dir / output_filename


def process_single_file(csv_file: Path, template_path: Path, output_dir: Path) -> dict:
    """
    Process a single CSV file with the template.
    
    Args:
        csv_file: Path to CSV file
        template_path: Path to JSON template
        output_dir: Output directory
        
    Returns:
        dict: Processing result
    """
    logger.info(f"Processing file: {csv_file.name}")
    
    # Generate output path
    output_path = generate_output_path(csv_file, template_path, output_dir)
    
    # Create settings
    settings = Settings(
        input_csv_path=csv_file,
        input_json_path=template_path,
        output_json_path=output_path
    )
    
    # Run the flow
    try:
        result = label_population_flow(
            input_csv_path=csv_file,
            input_json_path=template_path,
            output_json_path=output_path,
            settings=settings
        )
        
        if result['status'] == 'success':
            logger.info(f"Successfully processed {csv_file.name}")
            logger.info(f"Changes made: {result['changes_made']}")
            logger.info(f"Output saved to: {result['output_path']}")
        else:
            logger.error(f"Failed to process {csv_file.name}: {result['error']}")
            
        return result
        
    except Exception as e:
        logger.error(f"Error processing {csv_file.name}: {str(e)}")
        return {
            "status": "error",
            "error": str(e),
            "file": str(csv_file)
        }


def main():
    """Main function to run the label population process."""
    logger.info("Starting Label Population Script")
    logger.info("=" * 50)
    
    # Determine CSV path from configuration
    if 'CSV_FILE' in globals() and CSV_FILE:
        csv_path = CSV_FILE
        logger.info(f"Using single CSV file: {csv_path}")
    elif 'CSV_FOLDER' in globals() and CSV_FOLDER:
        csv_path = CSV_FOLDER
        logger.info(f"Using CSV folder: {csv_path}")
    else:
        logger.error("Please configure either CSV_FILE or CSV_FOLDER at the top of the script")
        sys.exit(1)
    
    # Validate inputs
    if not validate_inputs(csv_path, TEMPLATE_FILE):
        sys.exit(1)
    
    # Get CSV files
    csv_files = get_csv_files(csv_path)
    if not csv_files:
        logger.error("No CSV files to process")
        sys.exit(1)
    
    logger.info(f"Found {len(csv_files)} CSV file(s) to process")
    logger.info(f"Using template: {TEMPLATE_FILE}")
    logger.info(f"Output directory: {OUTPUT_DIR}")
    logger.info("=" * 50)
    
    # Process files
    results = []
    successful = 0
    failed = 0
    
    for i, csv_file in enumerate(csv_files, 1):
        logger.info(f"Processing file {i}/{len(csv_files)}: {csv_file.name}")
        
        result = process_single_file(csv_file, TEMPLATE_FILE, OUTPUT_DIR)
        results.append(result)
        
        if result['status'] == 'success':
            successful += 1
        else:
            failed += 1
    
    # Summary
    logger.info("=" * 50)
    logger.info("PROCESSING SUMMARY")
    logger.info("=" * 50)
    logger.info(f"Total files processed: {len(csv_files)}")
    logger.info(f"Successful: {successful}")
    logger.info(f"Failed: {failed}")
    logger.info(f"Output directory: {OUTPUT_DIR.absolute()}")
    
    if failed > 0:
        logger.warning(f"{failed} file(s) failed to process. Check the log for details.")
        sys.exit(1)
    else:
        logger.info("All files processed successfully!")


if __name__ == "__main__":
    main() 