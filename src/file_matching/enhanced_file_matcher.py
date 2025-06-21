#!/usr/bin/env python3
"""
Enhanced File Matcher Script

This script compares PDF files in the vgb_training_data/data folder with 
CSV ground truth files in the vgb_training_data/gt folder to identify:
- Files that have data but no ground truth
- Files that have ground truth but no data
- Files that have both (matched)
- Case sensitivity issues
"""

import os
import glob
from pathlib import Path
from typing import Set, Tuple, Dict, List


class EnhancedFileMatcher:
    def __init__(self, data_dir: str, gt_dir: str):
        """
        Initialize the enhanced file matcher.
        
        Args:
            data_dir: Path to the data directory containing PDF files
            gt_dir: Path to the ground truth directory containing CSV files
        """
        self.data_dir = Path(data_dir)
        self.gt_dir = Path(gt_dir)
        
    def get_data_files(self) -> Dict[str, str]:
        """Get all PDF files from the data directory with their original names."""
        pdf_files = glob.glob(str(self.data_dir / "*.pdf"))
        # Return dict with lowercase key and original filename as value
        return {Path(f).stem.lower(): Path(f).stem for f in pdf_files}
    
    def get_gt_files(self) -> Dict[str, str]:
        """Get all CSV files from the ground truth directory with their original names."""
        csv_files = glob.glob(str(self.gt_dir / "*.csv"))
        # Return dict with lowercase key and original filename as value
        return {Path(f).stem.lower(): Path(f).stem for f in csv_files}
    
    def analyze_files(self) -> Dict[str, any]:
        """
        Analyze the files and categorize them with case sensitivity handling.
        
        Returns:
            Dictionary with detailed analysis results
        """
        data_files = self.get_data_files()
        gt_files = self.get_gt_files()
        
        # Find exact matches (case-insensitive)
        data_keys = set(data_files.keys())
        gt_keys = set(gt_files.keys())
        
        matched_keys = data_keys.intersection(gt_keys)
        
        # Find files that only exist in data directory
        data_only_keys = data_keys - gt_keys
        
        # Find files that only exist in ground truth directory
        gt_only_keys = gt_keys - data_keys
        
        # Create detailed results
        matched = {key: (data_files[key], gt_files[key]) for key in matched_keys}
        data_only = {key: data_files[key] for key in data_only_keys}
        gt_only = {key: gt_files[key] for key in gt_only_keys}
        
        # Check for case sensitivity issues
        case_issues = []
        for data_key, data_name in data_files.items():
            for gt_key, gt_name in gt_files.items():
                if data_key == gt_key and data_name != gt_name:
                    case_issues.append((data_name, gt_name))
        
        return {
            'matched': matched,
            'data_only': data_only,
            'gt_only': gt_only,
            'case_issues': case_issues,
            'total_data': len(data_files),
            'total_gt': len(gt_files),
            'total_matched': len(matched)
        }
    
    def print_report(self, analysis: Dict[str, any]) -> None:
        """Print a detailed report of the file analysis."""
        print("=" * 70)
        print("ENHANCED FILE MATCHING ANALYSIS REPORT")
        print("=" * 70)
        print()
        
        # Summary statistics
        print("SUMMARY:")
        print(f"  Total data files (PDFs): {analysis['total_data']}")
        print(f"  Total ground truth files (CSVs): {analysis['total_gt']}")
        print(f"  Matched files: {analysis['total_matched']}")
        print(f"  Data files without ground truth: {len(analysis['data_only'])}")
        print(f"  Ground truth files without data: {len(analysis['gt_only'])}")
        print(f"  Case sensitivity issues: {len(analysis['case_issues'])}")
        print()
        
        # Case sensitivity issues
        if analysis['case_issues']:
            print("⚠ CASE SENSITIVITY ISSUES (same file, different case):")
            for data_name, gt_name in analysis['case_issues']:
                print(f"  - Data: {data_name} | GT: {gt_name}")
            print()
        
        # Detailed breakdown
        if analysis['matched']:
            print("✓ MATCHED FILES (have both data and ground truth):")
            for key, (data_name, gt_name) in sorted(analysis['matched'].items()):
                if data_name == gt_name:
                    print(f"  - {data_name}")
                else:
                    print(f"  - {data_name} (GT: {gt_name})")
            print()
        
        if analysis['data_only']:
            print("⚠ DATA FILES WITHOUT GROUND TRUTH:")
            for key, name in sorted(analysis['data_only'].items()):
                print(f"  - {name}")
            print()
        
        if analysis['gt_only']:
            print("⚠ GROUND TRUTH FILES WITHOUT DATA:")
            for key, name in sorted(analysis['gt_only'].items()):
                print(f"  - {name}")
            print()
        
        # Recommendations
        print("RECOMMENDATIONS:")
        if analysis['case_issues']:
            print(f"  - Fix {len(analysis['case_issues'])} case sensitivity issues")
        if analysis['data_only']:
            print(f"  - {len(analysis['data_only'])} PDF files need ground truth CSV files")
        if analysis['gt_only']:
            print(f"  - {len(analysis['gt_only'])} CSV files need corresponding PDF files")
        if not analysis['data_only'] and not analysis['gt_only'] and not analysis['case_issues']:
            print("  - All files are properly matched! ✓")
        
        print("=" * 70)
    
    def generate_missing_files_report(self, analysis: Dict[str, any]) -> None:
        """Generate a report of missing files that can be used for action."""
        print("\n" + "=" * 50)
        print("MISSING FILES ACTION REPORT")
        print("=" * 50)
        
        if analysis['data_only']:
            print("\nPDF files that need ground truth CSV files:")
            for key, name in sorted(analysis['data_only'].items()):
                print(f"  {name}.csv")
        
        if analysis['gt_only']:
            print("\nCSV files that need corresponding PDF files:")
            for key, name in sorted(analysis['gt_only'].items()):
                print(f"  {name}.pdf")
        
        if analysis['case_issues']:
            print("\nCase sensitivity issues to fix:")
            for data_name, gt_name in analysis['case_issues']:
                print(f"  Rename: {gt_name}.csv -> {data_name}.csv")


def main():
    """Main function to run the enhanced file matching analysis."""
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
    matcher = EnhancedFileMatcher(data_dir, gt_dir)
    analysis = matcher.analyze_files()
    
    # Print reports
    matcher.print_report(analysis)
    matcher.generate_missing_files_report(analysis)


if __name__ == "__main__":
    main() 