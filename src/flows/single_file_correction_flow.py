from prefect import flow
from pathlib import Path
from tasks.label_correction_tasks import (
    load_ground_truth,
    load_labels_json,
    group_labels,
    find_corrections,
    reconstruct_labels,
    save_corrected_json,
    save_correction_report
)

@flow(name="single_file_correction_flow")
def single_file_correction_flow(
    ground_truth_path: Path,
    labels_json_path: Path,
    output_json_path: Path,
    correction_report_path: Path
) -> None:
    """
    Flow to correct labels in a single JSON file based on ground truth CSV data.
    
    Args:
        ground_truth_path: Path to the ground truth CSV file
        labels_json_path: Path to the input labels JSON file
        output_json_path: Path where the corrected JSON will be saved
        correction_report_path: Path where the correction report will be saved
    """
    # Load input data
    ground_truth_df = load_ground_truth(ground_truth_path)
    labels_data = load_labels_json(labels_json_path)
    
    # Process the data
    grouped_labels = group_labels(labels_data)
    updated_labels, corrections = find_corrections(grouped_labels, ground_truth_df)
    
    # Reconstruct and save the updated labels
    updated_labels_list = reconstruct_labels(updated_labels)
    save_corrected_json(labels_data, updated_labels_list, output_json_path)
    
    # Save correction report if there are any corrections
    save_correction_report(corrections, correction_report_path) 