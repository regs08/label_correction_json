from prefect import task
import pandas as pd
from pathlib import Path
from typing import Dict, List, Any, Tuple
from src.models.data_models import LabelsData, LabelEntry, LabelValue
from src.utils.config import Settings

@task(name="process_csv_data")
def process_csv_data(df: pd.DataFrame, settings: Settings) -> Dict[int, Dict[str, str]]:
    """Process CSV data into a format suitable for JSON updates."""
    processed_data = {}
    
    print("\nProcessing CSV data:")
    print(f"Total rows in CSV: {len(df)}")
    
    # Process every row
    for idx, row in df.iterrows():
        row_data = {}
        for col in df.columns:
            value = str(row[col])
            # Handle blank spaces and NaN values
            if pd.isna(row[col]) or value.strip() == "":
                value = ""
            # Handle zeros (both "0" and "0.0")
            elif value == "0" or value == "0.0":
                value = "-"
            row_data[col] = value
        
        processed_data[idx] = row_data
    
    return processed_data

@task(name="update_json_data")
def update_json_data(
    labels_data: LabelsData,
    processed_csv: Dict[int, Dict[str, str]],
    settings: Settings
) -> Tuple[LabelsData, int]:
    """Update JSON data with values from CSV."""
    updated_labels = []
    changes_made = 0
    
    for entry in labels_data.labels:
        if entry.label.startswith("DynamicTable/"):
            parts = entry.label.split("/")
            if len(parts) == 3:
                row_idx = int(parts[1])
                field = parts[2]  # Use the field name as is
                
                if row_idx in processed_csv and field in processed_csv[row_idx]:
                    new_value = processed_csv[row_idx][field]
                    # Convert new_value to lowercase
                    new_value = new_value.lower()
                    # Always update if the new value is an empty string
                    if new_value == "" or entry.value[0].text != new_value:
                        print(f"Updating {entry.label}:")
                        print(f"Old value: {entry.value[0].text}")
                        print(f"New value: {new_value}")
                        entry.value[0].text = new_value
                        changes_made += 1
        
        updated_labels.append(entry)
    
    print(f"\nTotal changes made: {changes_made}")
    labels_data.labels = updated_labels
    return labels_data, changes_made

@task(name="clean_selected_values")
def clean_selected_values(labels_data: LabelsData) -> Tuple[LabelsData, int]:
    """Clean up any remaining 'selected' values in the JSON."""
    changes_made = 0
    
    print("\nCleaning up 'selected' values:")
    for entry in labels_data.labels:
        if entry.value[0].text == "selected":
            print(f"Replacing 'selected' with empty string in {entry.label}")
            entry.value[0].text = ""
            changes_made += 1
    
    print(f"Total 'selected' values cleaned: {changes_made}")
    return labels_data, changes_made 