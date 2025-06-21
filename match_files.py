#!/usr/bin/env python3
"""
Simple script to run file matching analysis between VGB data and ground truth files.
"""

import sys
from pathlib import Path

# Add src to path so we can import our modules
sys.path.append(str(Path(__file__).parent / "src"))

from file_matching.file_matcher import main

if __name__ == "__main__":
    main() 