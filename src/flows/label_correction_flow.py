from prefect import flow
from pathlib import Path
from typing import Dict, List, Tuple
from src.utils.config import CompareFilesConfig
from src.tasks.label_correction_tasks import (
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

@flow(name="batch_correction_flow")
def batch_correction_flow(config: CompareFilesConfig) -> Dict[str, List[Path]]:
    """
    Batch process multiple JSON labels files against their corresponding ground truth CSV files.
    
    Args:
        config: CompareFilesConfig containing matched file pairs and output paths
        
    Returns:
        Dictionary containing:
        - corrected_files: List of paths to corrected JSON files
        - report_files: List of paths to correction report CSV files
        - total_files_processed: Total number of file pairs processed
        - total_files_corrected: Number of files that required corrections
        - total_reports_generated: Number of correction reports generated
    """
    corrected_files = []
    report_files = []
    
    for json_path, csv_path in config.matched_files:
        # Load data
        ground_truth_df = load_ground_truth(csv_path)
        labels_data = load_labels_json(json_path)
        
        # Process labels
        grouped_labels = group_labels(labels_data)
        updated_labels, corrections = find_corrections(grouped_labels, ground_truth_df)
        reconstructed_labels = reconstruct_labels(updated_labels)
        
        # Save results
        output_path = config.get_output_path(json_path)
        corrected_path = save_corrected_json(
            labels_data=labels_data,
            updated_labels=reconstructed_labels,
            output_path=output_path
        )
        corrected_files.append(corrected_path)
        
        # Save report if there are corrections
        if corrections:
            report_path = config.get_report_path(json_path)
            saved_report = save_correction_report(corrections, report_path)
            if saved_report:
                report_files.append(saved_report)
    
    return {
        "corrected_files": corrected_files,
        "report_files": report_files,
        "total_files_processed": len(config.matched_files),
        "total_files_corrected": len(corrected_files),
        "total_reports_generated": len(report_files)
    } 