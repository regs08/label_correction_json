#!/usr/bin/env python3
"""
Integrated Label Population Script

This script matches CSV files with JSON files and populates labels automatically.
It processes corresponding JSON files for each CSV file.
"""

from pathlib import Path
from src.flows.integrated_label_population_flow import integrated_label_population_flow

def main():
    # Configuration - modify these paths as needed
    csv_dir = Path("/Users/cole/PycharmProjects/label_correction_json/vgb_training_data/gt")
    json_dir = Path("/Users/cole/PycharmProjects/label_correction_json/vgb_training_data/input_labels")
    output_dir = Path("output")
    
    print("=" * 60)
    print("INTEGRATED LABEL POPULATION")
    print("=" * 60)
    print(f"CSV Directory: {csv_dir}")
    print(f"JSON Directory: {json_dir}")
    print(f"Output Directory: {output_dir}")
    print("=" * 60)
    
    # Run the integrated flow
    result = integrated_label_population_flow(
        csv_dir=csv_dir,
        json_dir=json_dir,
        output_dir=output_dir,
        template_json_path=None  # Use corresponding JSON files
    )
    
    # Print final summary
    print(f"\n{'='*60}")
    print("FINAL SUMMARY")
    print(f"{'='*60}")
    print(f"Overall Status: {result['status']}")
    print(f"Total Files Processed: {result['total_files_processed']}")
    print(f"Successful: {result['successful_processing']}")
    print(f"Failed: {result['failed_processing']}")
    
    if result['successful_processing'] > 0:
        print(f"\n✅ Successfully processed {result['successful_processing']} files!")
        print(f"Output files saved to: {output_dir}")
    
    if result['failed_processing'] > 0:
        print(f"\n❌ {result['failed_processing']} files failed to process.")
        print("Check the errors above for details.")

if __name__ == "__main__":
    main() 