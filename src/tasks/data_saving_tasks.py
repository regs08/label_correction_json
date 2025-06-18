from prefect import task
import json
from pathlib import Path
from src.models.data_models import LabelsData, ProcessingResult

@task(name="save_updated_json")
def save_updated_json(labels_data: LabelsData, output_path: Path) -> Path:
    """Save updated JSON data to file."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Convert LabelsData to dictionary
    data = {
        "$schema": labels_data.schema,
        "document": labels_data.document,
        "labels": [
            {
                "label": entry.label,
                "value": [
                    {
                        "page": value.page,
                        "text": value.text,
                        "boundingBoxes": value.boundingBoxes
                    }
                    for value in entry.value
                ]
            }
            for entry in labels_data.labels
        ]
    }
    
    with open(output_path, 'w') as f:
        json.dump(data, f, indent=2)
    
    return output_path

@task(name="generate_processing_report")
def generate_processing_report(
    input_csv_path: Path,
    input_json_path: Path,
    output_json_path: Path,
    changes_made: int,
    errors: list[str] = None
) -> ProcessingResult:
    """Generate a report of the processing results."""
    return ProcessingResult(
        input_csv_path=input_csv_path,
        input_json_path=input_json_path,
        output_json_path=output_json_path,
        changes_made=changes_made,
        error_count=len(errors) if errors else 0,
        errors=errors or []
    ) 