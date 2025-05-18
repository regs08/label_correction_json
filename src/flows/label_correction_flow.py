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

@flow(name="label_correction_flow")
def label_correction_flow(config: CompareFilesConfig) -> Dict[str, List[Path]]:
    """
    Compare JSON labels with ground truth CSV files and generate corrections.
    
    Args:
        config: CompareFilesConfig containing matched file pairs and output paths
        
    Returns:
        Dictionary containing:
        - corrected_files: List of paths to corrected JSON files
        - report_files: List of paths to correction report CSV files
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