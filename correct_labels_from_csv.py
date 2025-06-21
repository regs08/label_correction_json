import json
import pandas as pd
from pathlib import Path

def correct_labels_json(labels_json_path, ground_truth_csv_path, output_json_path, report_csv_path):
    # Load the ground truth CSV
    ground_truth_df = pd.read_csv(ground_truth_csv_path)

    # Load the labels JSON
    with open(labels_json_path, "r") as f:
        labels_data = json.load(f)

    label_entries = labels_data["labels"]

    # Group labels by dynamic index
    from collections import defaultdict
    grouped_labels = defaultdict(dict)
    for entry in label_entries:
        label = entry["label"]
        value = entry["value"][0]
        if label.startswith("dynamic/"):
            parts = label.split("/")
            group_idx = int(parts[1])
            field = parts[2]
            # Convert field name to lowercase for consistency
            field_lower = field.lower()
            grouped_labels[group_idx][field_lower] = value

    # Correct text values
    corrections = []
    for idx, row in ground_truth_df.iterrows():
        if idx not in grouped_labels:
            continue
        for field, val in row.items():
            # Convert field name to lowercase for lookup
            field_lower = field.lower()
            if field_lower in grouped_labels[idx] and pd.notna(val):
                current_text = grouped_labels[idx][field_lower]["text"]
                corrected_text = str(val)
                # Convert corrected_text to lowercase
                corrected_text = corrected_text.lower()
                if current_text != corrected_text:
                    corrections.append({
                        "Label": f"dynamic/{idx}/{field_lower}",
                        "Original": current_text,
                        "Corrected": corrected_text
                    })
                    grouped_labels[idx][field_lower]["text"] = corrected_text

    # Reconstruct updated labels
    updated_labels = []
    for group_idx, fields in grouped_labels.items():
        for field, val in fields.items():
            # Convert field name to lowercase
            field_lower = field.lower()
            updated_labels.append({
                "label": f"dynamic/{group_idx}/{field_lower}",
                "value": [val]
            })

    # Save corrected JSON
    labels_data["labels"] = updated_labels
    with open(output_json_path, "w") as f:
        json.dump(labels_data, f, indent=2)

    # Save correction report
    if corrections:
        corrections_df = pd.DataFrame(corrections)
        corrections_df.to_csv(report_csv_path, index=False)

    return output_json_path, report_csv_path

# Example usage:
if __name__ == "__main__":
    # Get the current working directory
    cwd = Path.cwd()
    
    # Define paths relative to the current working directory
    labels_json = cwd / "example_data/labels/TEST_20250505_R7P4_R8P2.pdf.labels.json"
    ground_truth_csv = cwd / "example_data/ground_truth/TEST_gt_20250505_R7P4_R8P2.csv"
    output_json = cwd / "corrected_test_20250605_r8p3_r9p1.labels.json"
    report_csv = cwd / "correction_report.csv"
    
    # Convert Path objects to strings for the function call
    correct_labels_json(
        str(labels_json),
        str(ground_truth_csv),
        str(output_json),
        str(report_csv)

    )
