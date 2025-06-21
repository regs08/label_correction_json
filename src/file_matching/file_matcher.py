#!/usr/bin/env python3
"""
File Matcher Script

This script compares PDF files in the vgb_training_data/data folder with 
CSV ground truth files in the vgb_training_data/gt folder to identify:
- Files that have data but no ground truth
- Files that have ground truth but no data
- Files that have both (matched)
"""

import os
import glob
from pathlib import Path
from typing import Set, Tuple, Dict, List


class FileMatcher:
    def __init__(self, data_dir: str, gt_dir: str):
        """
        Initialize the file matcher.
        
        Args:
            data_dir: Path to the data directory containing PDF files
            gt_dir: Path to the ground truth directory containing CSV files
        """
        self.data_dir = Path(data_dir)
        self.gt_dir = Path(gt_dir)
        
    def get_data_files(self) -> Set[str]:
        """Get all PDF files from the data directory."""
        pdf_files = glob.glob(str(self.data_dir / "*.pdf"))
        # Extract just the filename without extension
        return {Path(f).stem for f in pdf_files}
    
    def get_gt_files(self) -> Set[str]:
        """Get all CSV files from the ground truth directory."""
        csv_files = glob.glob(str(self.gt_dir / "*.csv"))
        # Extract just the filename without extension
        return {Path(f).stem for f in csv_files}
    
    def analyze_files(self) -> Dict[str, Set[str]]:
        """
        Analyze the files and categorize them.
        
        Returns:
            Dictionary with categories: 'data_only', 'gt_only', 'matched'
        """
        data_files = self.get_data_files()
        gt_files = self.get_gt_files()
        
        # Find matches (files that exist in both directories)
        matched = data_files.intersection(gt_files)
        
        # Find files that only exist in data directory
        data_only = data_files - gt_files
        
        # Find files that only exist in ground truth directory
        gt_only = gt_files - data_files
        
        return {
            'matched': matched,
            'data_only': data_only,
            'gt_only': gt_only
        }
    
    def print_report(self, analysis: Dict[str, Set[str]]) -> None:
        """Print a detailed report of the file analysis."""
        print("=" * 60)
        print("FILE MATCHING ANALYSIS REPORT")
        print("=" * 60)
        print()
        
        # Summary statistics
        print("SUMMARY:")
        print(f"  Total data files (PDFs): {len(analysis['matched']) + len(analysis['data_only'])}")
        print(f"  Total ground truth files (CSVs): {len(analysis['matched']) + len(analysis['gt_only'])}")
        print(f"  Matched files: {len(analysis['matched'])}")
        print(f"  Data files without ground truth: {len(analysis['data_only'])}")
        print(f"  Ground truth files without data: {len(analysis['gt_only'])}")
        print()
        
        # Detailed breakdown
        if analysis['matched']:
            print("✓ MATCHED FILES (have both data and ground truth):")
            for file in sorted(analysis['matched']):
                print(f"  - {file}")
            print()
        
        if analysis['data_only']:
            print("⚠ DATA FILES WITHOUT GROUND TRUTH:")
            for file in sorted(analysis['data_only']):
                print(f"  - {file}")
            print()
        
        if analysis['gt_only']:
            print("⚠ GROUND TRUTH FILES WITHOUT DATA:")
            for file in sorted(analysis['gt_only']):
                print(f"  - {file}")
            print()
        
        # Recommendations
        print("RECOMMENDATIONS:")
        if analysis['data_only']:
            print(f"  - {len(analysis['data_only'])} PDF files need ground truth CSV files")
        if analysis['gt_only']:
            print(f"  - {len(analysis['gt_only'])} CSV files need corresponding PDF files")
        if not analysis['data_only'] and not analysis['gt_only']:
            print("  - All files are properly matched! ✓")
        
        print("=" * 60)


def main():
    """Main function to run the file matching analysis."""
    # Define paths
    project_root = Path(__file__).parent.parent.parent
    data_dir = project_root / "vgb_training_data" / "data"
    gt_dir = project_root / "vgb_training_data" / "gt"
    
    # Check if directories exist
    if not data_dir.exists():
        print(f"Error: Data directory not found: {data_dir}")
        return
    
    if not gt_dir.exists():
        print(f"Error: Ground truth directory not found: {gt_dir}")
        return
    
    # Create file matcher and run analysis
    matcher = FileMatcher(data_dir, gt_dir)
    analysis = matcher.analyze_files()
    
    # Print report
    matcher.print_report(analysis)


if __name__ == "__main__":
    main() 