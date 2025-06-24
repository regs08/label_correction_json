#!/usr/bin/env python3
"""
JSON-CSV File Matcher

This script matches JSON label files with CSV ground truth files to identify:
- Files that have CSV but no JSON
- Files that have JSON but no CSV
- Files that have both (matched)
"""

import os
import glob
from pathlib import Path
from typing import Set, Tuple, Dict, List


class JsonCsvMatcher:
    def __init__(self, json_dir: str, csv_dir: str):
        """
        Initialize the JSON-CSV file matcher.
        
        Args:
            json_dir: Path to the JSON directory containing label files
            csv_dir: Path to the CSV directory containing ground truth files
        """
        self.json_dir = Path(json_dir)
        self.csv_dir = Path(csv_dir)
        
    def get_json_files(self) -> Set[str]:
        """Get all JSON files from the JSON directory."""
        json_files = glob.glob(str(self.json_dir / "*.json"))
        json_stems = set()
        for f in json_files:
            name = Path(f).name
            if name.endswith('.pdf.labels.json'):
                stem = name[:-len('.pdf.labels.json')]
            else:
                stem = Path(f).stem
            json_stems.add(stem)
        return json_stems
    
    def get_csv_files(self) -> Set[str]:
        """Get all CSV files from the CSV directory."""
        csv_files = glob.glob(str(self.csv_dir / "*.csv"))
        # Extract just the filename without extension
        return {Path(f).stem for f in csv_files}
    
    def analyze_files(self) -> Dict[str, Set[str]]:
        """
        Analyze the files and categorize them.
        
        Returns:
            Dictionary with categories: 'json_only', 'csv_only', 'matched'
        """
        json_files = self.get_json_files()
        csv_files = self.get_csv_files()
        
        # Find matches (files that exist in both directories)
        matched = json_files.intersection(csv_files)
        
        # Find files that only exist in JSON directory
        json_only = json_files - csv_files
        
        # Find files that only exist in CSV directory
        csv_only = csv_files - json_files
        
        return {
            'matched': matched,
            'json_only': json_only,
            'csv_only': csv_only
        }
    
    def print_report(self, analysis: Dict[str, Set[str]]) -> None:
        """Print a detailed report of the file analysis."""
        print("=" * 60)
        print("JSON-CSV FILE MATCHING ANALYSIS REPORT")
        print("=" * 60)
        print()
        
        # Summary statistics
        print("SUMMARY:")
        print(f"  Total JSON files: {len(analysis['matched']) + len(analysis['json_only'])}")
        print(f"  Total CSV files: {len(analysis['matched']) + len(analysis['csv_only'])}")
        print(f"  Matched files: {len(analysis['matched'])}")
        print(f"  JSON files without CSV: {len(analysis['json_only'])}")
        print(f"  CSV files without JSON: {len(analysis['csv_only'])}")
        print()
        
        # Detailed breakdown
        if analysis['matched']:
            print("✓ MATCHED FILES (have both JSON and CSV):")
            for file in sorted(analysis['matched']):
                print(f"  - {file}")
            print()
        
        if analysis['json_only']:
            print("⚠ JSON FILES WITHOUT CSV:")
            for file in sorted(analysis['json_only']):
                print(f"  - {file}")
            print()
        
        if analysis['csv_only']:
            print("⚠ CSV FILES WITHOUT JSON:")
            for file in sorted(analysis['csv_only']):
                print(f"  - {file}")
            print()
        
        # Recommendations
        print("RECOMMENDATIONS:")
        if analysis['json_only']:
            print(f"  - {len(analysis['json_only'])} JSON files need corresponding CSV files")
        if analysis['csv_only']:
            print(f"  - {len(analysis['csv_only'])} CSV files need corresponding JSON files")
        if not analysis['json_only'] and not analysis['csv_only']:
            print("  - All files are properly matched! ✓")
        
        print("=" * 60)


def main():
    """Main function to run the JSON-CSV file matching analysis."""
    # Define paths
    project_root = Path(__file__).parent.parent.parent
    json_dir = project_root / "vgb_training_data" / "input_labels"
    csv_dir = project_root / "vgb_training_data" / "gt"
    
    # Check if directories exist
    if not json_dir.exists():
        print(f"Error: JSON directory not found: {json_dir}")
        return
    
    if not csv_dir.exists():
        print(f"Error: CSV directory not found: {csv_dir}")
        return
    
    # Create file matcher and run analysis
    matcher = JsonCsvMatcher(json_dir, csv_dir)
    analysis = matcher.analyze_files()
    
    # Print report
    matcher.print_report(analysis)


if __name__ == "__main__":
    main() 