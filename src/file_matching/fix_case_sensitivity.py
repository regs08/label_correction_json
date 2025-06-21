#!/usr/bin/env python3
"""
Case Sensitivity Fix Script

This script automatically fixes case sensitivity issues by renaming CSV files
in the ground truth directory to match the lowercase naming convention used
in the data directory.
"""

import os
import glob
from pathlib import Path
from typing import List, Tuple


class CaseSensitivityFixer:
    def __init__(self, gt_dir: str):
        """
        Initialize the case sensitivity fixer.
        
        Args:
            gt_dir: Path to the ground truth directory containing CSV files
        """
        self.gt_dir = Path(gt_dir)
        
    def find_case_issues(self) -> List[Tuple[str, str]]:
        """
        Find CSV files that have uppercase 'VGB' instead of lowercase 'vgb'.
        
        Returns:
            List of tuples (old_name, new_name)
        """
        csv_files = glob.glob(str(self.gt_dir / "*.csv"))
        case_issues = []
        
        for csv_file in csv_files:
            filename = Path(csv_file).name
            if filename.startswith("VGB_"):
                # Convert VGB_ to vgb_
                new_filename = filename.replace("VGB_", "vgb_", 1)
                case_issues.append((filename, new_filename))
        
        return case_issues
    
    def fix_case_issues(self, dry_run: bool = True) -> None:
        """
        Fix case sensitivity issues by renaming files.
        
        Args:
            dry_run: If True, only show what would be done without actually renaming
        """
        case_issues = self.find_case_issues()
        
        if not case_issues:
            print("No case sensitivity issues found!")
            return
        
        print(f"Found {len(case_issues)} case sensitivity issues:")
        print()
        
        for old_name, new_name in case_issues:
            old_path = self.gt_dir / old_name
            new_path = self.gt_dir / new_name
            
            if dry_run:
                print(f"Would rename: {old_name} -> {new_name}")
            else:
                try:
                    old_path.rename(new_path)
                    print(f"✓ Renamed: {old_name} -> {new_name}")
                except Exception as e:
                    print(f"✗ Error renaming {old_name}: {e}")
        
        if dry_run:
            print()
            print("This was a dry run. To actually fix the files, run with dry_run=False")
        else:
            print()
            print("✓ All case sensitivity issues have been fixed!")


def main():
    """Main function to run the case sensitivity fix."""
    # Define paths
    project_root = Path(__file__).parent.parent.parent
    gt_dir = project_root / "vgb_training_data" / "gt"
    
    # Check if directory exists
    if not gt_dir.exists():
        print(f"Error: Ground truth directory not found: {gt_dir}")
        return
    
    # Create fixer and run analysis
    fixer = CaseSensitivityFixer(gt_dir)
    
    # First, show what would be done (dry run)
    print("DRY RUN - Case Sensitivity Fix")
    print("=" * 40)
    fixer.fix_case_issues(dry_run=True)
    
    # Ask user if they want to proceed
    print()
    response = input("Do you want to proceed with fixing the case sensitivity issues? (y/N): ")
    
    if response.lower() in ['y', 'yes']:
        print()
        print("FIXING CASE SENSITIVITY ISSUES")
        print("=" * 40)
        fixer.fix_case_issues(dry_run=False)
    else:
        print("Operation cancelled.")


if __name__ == "__main__":
    main() 