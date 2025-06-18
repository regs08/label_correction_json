from prefect import task
import pandas as pd
import json
from pathlib import Path
from typing import Tuple, Dict, Any
from src.models.data_models import LabelsData, LabelEntry, LabelValue
from src.utils.config import Settings

@task(name="load_csv_data")
def load_csv_data(csv_path: Path, settings: Settings) -> pd.DataFrame:
    """Load and validate CSV data."""
    df = pd.read_csv(csv_path)
    
    # Validate required columns
    missing_columns = [col for col in settings.required_csv_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    return df

@task(name="load_json_data")
def load_json_data(json_path: Path) -> LabelsData:
    """Load and validate JSON data."""
    with open(json_path, 'r') as f:
        data = json.load(f)
    
    # Convert JSON data to LabelsData
    labels = []
    for entry in data['labels']:
        values = []
        for value in entry['value']:
            values.append(LabelValue(
                page=value['page'],
                text=value['text'],
                boundingBoxes=value['boundingBoxes']
            ))
        labels.append(LabelEntry(
            label=entry['label'],
            value=values
        ))
    
    return LabelsData(
        schema=data['$schema'],
        document=data['document'],
        labels=labels
    )

@task(name="validate_input_files")
def validate_input_files(csv_path: Path, json_path: Path) -> Tuple[bool, str]:
    """Validate that input files exist and are accessible."""
    if not csv_path.exists():
        return False, f"CSV file not found: {csv_path}"
    if not json_path.exists():
        return False, f"JSON file not found: {json_path}"
    return True, "Files validated successfully" 