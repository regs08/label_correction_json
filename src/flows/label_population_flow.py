from prefect import flow
from pathlib import Path
from typing import Optional, Dict, Any
from src.tasks.data_loading_tasks import load_csv_data, load_json_data, validate_input_files
from src.tasks.data_processing_tasks import process_csv_data, update_json_data, clean_selected_values
from src.tasks.data_saving_tasks import save_updated_json, generate_processing_report
from src.utils.config import Settings

@flow(name="label_population_flow")
def label_population_flow(
    input_csv_path: Path,
    input_json_path: Path,
    output_json_path: Optional[Path] = None,
    settings: Optional[Settings] = None
) -> Dict[str, Any]:
    """
    Main flow for populating labels.json with values from CSV.
    
    Args:
        input_csv_path: Path to input CSV file
        input_json_path: Path to input JSON file
        output_json_path: Path to save output JSON (optional)
        settings: Settings object (optional)
    
    Returns:
        dict: Processing results and statistics
    """
    # Initialize settings
    if settings is None:
        settings = Settings(
            input_csv_path=input_csv_path,
            input_json_path=input_json_path,
            output_json_path=output_json_path or input_json_path.parent / f"updated_{input_json_path.name.lower()}"
        )
    
    # Set default output path if not provided
    if output_json_path is None:
        output_json_path = input_json_path.parent / f"updated_{input_json_path.name.lower()}"
    
    # Validate input files
    is_valid, message = validate_input_files(input_csv_path, input_json_path)
    if not is_valid:
        return {"status": "error", "error": message}
    
    try:
        # Load data
        df = load_csv_data(input_csv_path, settings)
        labels_data = load_json_data(input_json_path)
        
        # Process data
        processed_csv = process_csv_data(df, settings)
        updated_labels, changes_made = update_json_data(labels_data, processed_csv, settings)
        
        # Clean up any remaining 'selected' values
        updated_labels, selected_cleaned = clean_selected_values(updated_labels)
        changes_made += selected_cleaned
        
        # Save results
        saved_path = save_updated_json(updated_labels, output_json_path)
        
        # Generate report
        report = generate_processing_report(
            input_csv_path=input_csv_path,
            input_json_path=input_json_path,
            output_json_path=saved_path,
            changes_made=changes_made
        )
        
        return {
            "status": "success",
            "changes_made": changes_made,
            "output_path": str(saved_path),
            "report": {
                "input_csv_path": str(report.input_csv_path),
                "input_json_path": str(report.input_json_path),
                "output_json_path": str(report.output_json_path),
                "changes_made": report.changes_made,
                "error_count": report.error_count,
                "errors": report.errors
            }
        }
        
    except Exception as e:
        return {
            "status": "error",
            "error": str(e)
        } 