#!/usr/bin/env python3
"""
Script to fix case sensitivity issues in VGB ground truth files.
"""

import sys
from pathlib import Path

# Add src to path so we can import our modules
sys.path.append(str(Path(__file__).parent / "src"))

from file_matching.fix_case_sensitivity import main

if __name__ == "__main__":
    main() 