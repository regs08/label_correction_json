from dataclasses import dataclass
from typing import List, Dict, Any
from pathlib import Path

@dataclass
class LabelValue:
    """Model for a single label value in the JSON structure."""
    page: int
    text: str
    boundingBoxes: List[Dict[str, float]]

@dataclass
class LabelEntry:
    """Model for a single label entry in the JSON structure."""
    label: str
    value: List[LabelValue]

@dataclass
class LabelsData:
    """Model for the complete labels JSON structure."""
    schema: str
    document: str
    labels: List[LabelEntry]

@dataclass
class ProcessingResult:
    """Model for the processing result."""
    input_csv_path: Path
    input_json_path: Path
    output_json_path: Path
    changes_made: int
    error_count: int = 0
    errors: List[str] = None

    def __post_init__(self):
        if self.errors is None:
            self.errors = [] 