from prefect import task
from pathlib import Path
import json
import pandas as pd
from typing import Dict, List, Tuple, Optional
from collections import defaultdict

@task(name="load_ground_truth")
def load_ground_truth(csv_path: Path) -> pd.DataFrame:
    """Load ground truth data from CSV file."""
    return pd.read_csv(csv_path)

@task(name="load_labels_json")
def load_labels_json(json_path: Path) -> Dict:
    """Load labels data from JSON file."""
    with open(json_path, "r") as f:
        return json.load(f)

@task(name="group_labels")
def group_labels(labels_data: Dict) -> Dict[int, Dict]:
    """Group labels by no header index."""
    grouped_labels = defaultdict(dict)
    for entry in labels_data["labels"]:
        label = entry["label"]
        value = entry["value"][0]  # Get the first value object
        if label.startswith("BBOX/"):
            parts = label.split("/")
            group_idx = int(parts[1])
            field = parts[2]
            grouped_labels[group_idx][field] = value
    return grouped_labels

@task(name="find_corrections")
def find_corrections(
    grouped_labels: Dict[int, Dict],
    ground_truth_df: pd.DataFrame
) -> Tuple[Dict[int, Dict], List[Dict]]:
    """Find and apply corrections to labels."""
    corrections = []
    for idx, row in ground_truth_df.iterrows():
        if idx not in grouped_labels:
            continue
        for field, val in row.items():
            if field in grouped_labels[idx] and pd.notna(val):
                current_value = grouped_labels[idx][field]
                current_text = current_value["text"]
                
                # Convert ground truth value to string, preserving decimals
                if isinstance(val, (int, float)):
                    # For R.P field, keep the decimal
                    if field == "R.P":
                        corrected_text = f"{val:.1f}"
                    else:
                        # For other fields, convert to integer
                        corrected_text = str(int(val))
                else:
                    corrected_text = str(val)
                
                if current_text != corrected_text:
                    corrections.append({
                        "Label": f"BBOX/{idx}/{field}",
                        "Original": current_text,
                        "Corrected": corrected_text
                    })
                    # Update only the text field, preserve other fields
                    current_value["text"] = corrected_text
    return grouped_labels, corrections

@task(name="reconstruct_labels")
def reconstruct_labels(grouped_labels: Dict[int, Dict]) -> List[Dict]:
    """Reconstruct labels list from grouped labels."""
    updated_labels = []
    for group_idx, fields in grouped_labels.items():
        for field, value in fields.items():
            updated_labels.append({
                "label": f"BBOX/{group_idx}/{field}",
                "value": [value]  # Preserve the original value structure
            })
    return updated_labels

@task(name="save_corrected_json")
def save_corrected_json(
    labels_data: Dict,
    updated_labels: List[Dict],
    output_path: Path
) -> Path:
    """Save corrected labels to JSON file."""
    # Create output directory if it doesn't exist
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    labels_data["labels"] = updated_labels
    with open(output_path, "w") as f:
        json.dump(labels_data, f, indent=2)
    return output_path

@task(name="save_correction_report")
def save_correction_report(
    corrections: List[Dict],
    report_path: Path
) -> Optional[Path]:
    """Save correction report to CSV file."""
    if corrections:
        # Create report directory if it doesn't exist
        report_path.parent.mkdir(parents=True, exist_ok=True)
        
        corrections_df = pd.DataFrame(corrections)
        corrections_df.to_csv(report_path, index=False, header=False)
        return report_path
    return None 