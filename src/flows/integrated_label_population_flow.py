from prefect import flow
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple
from src.file_matching.json_csv_matcher import JsonCsvMatcher
from src.flows.label_population_flow import label_population_flow
from src.utils.config import Settings


@flow(name="integrated_label_population_flow")
def integrated_label_population_flow(
    csv_dir: Path,
    json_dir: Path,
    output_dir: Optional[Path] = None,
    template_json_path: Optional[Path] = None
) -> Dict[str, Any]:
    """
    Integrated flow that matches CSV files with JSON files and populates labels.
    
    Args:
        csv_dir: Directory containing CSV ground truth files
        json_dir: Directory containing JSON label files (or template)
        output_dir: Directory to save output JSON files (optional)
        template_json_path: Path to template JSON file if using single template (optional)
    
    Returns:
        dict: Processing results and statistics
    """
    # Set default output directory
    if output_dir is None:
        output_dir = Path.cwd() / "output"
    
    # Create output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Initialize results tracking
    results = {
        "status": "success",
        "total_files_processed": 0,
        "successful_processing": 0,
        "failed_processing": 0,
        "matched_files": [],
        "csv_only_files": [],
        "json_only_files": [],
        "processing_results": [],
        "errors": []
    }
    
    try:
        # Create file matcher
        matcher = JsonCsvMatcher(json_dir, csv_dir)
        analysis = matcher.analyze_files()
        
        # Print file matching report
        matcher.print_report(analysis)
        
        # Track unmatched files
        results["csv_only_files"] = list(analysis['csv_only'])
        results["json_only_files"] = list(analysis['json_only'])
        
        # Process matched files
        matched_files = analysis['matched']
        results["matched_files"] = list(matched_files)
        results["total_files_processed"] = len(matched_files)
        
        print(f"\n{'='*60}")
        print(f"PROCESSING {len(matched_files)} MATCHED FILES")
        print(f"{'='*60}")
        
        for file_stem in sorted(matched_files):
            print(f"\nProcessing: {file_stem}")
            
            # Define file paths
            csv_path = csv_dir / f"{file_stem}.csv"
            
            if template_json_path:
                # Use single template for all files
                json_path = template_json_path
                output_json_path = output_dir / f"{file_stem}.pdf.labels.json"
            else:
                # Use corresponding JSON file
                json_path = json_dir / f"{file_stem}.pdf.labels.json"
                output_json_path = output_dir / f"{file_stem}.pdf.labels.json"
            
            # Check if files exist
            if not csv_path.exists():
                error_msg = f"CSV file not found: {csv_path}"
                print(f"❌ {error_msg}")
                results["errors"].append(error_msg)
                results["failed_processing"] += 1
                continue
                
            if not json_path.exists():
                error_msg = f"JSON file not found: {json_path}"
                print(f"❌ {error_msg}")
                results["errors"].append(error_msg)
                results["failed_processing"] += 1
                continue
            
            try:
                # Process the file pair
                file_result = label_population_flow(
                    input_csv_path=csv_path,
                    input_json_path=json_path,
                    output_json_path=output_json_path
                )
                
                # Track result
                file_summary = {
                    "file_stem": file_stem,
                    "csv_path": str(csv_path),
                    "json_path": str(json_path),
                    "output_path": str(output_json_path),
                    "status": file_result["status"],
                    "changes_made": file_result.get("changes_made", 0),
                    "error": file_result.get("error", None)
                }
                
                results["processing_results"].append(file_summary)
                
                if file_result["status"] == "success":
                    print(f"✅ Success: {file_result['changes_made']} changes made")
                    print(f"   Output: {output_json_path}")
                    results["successful_processing"] += 1
                else:
                    print(f"❌ Failed: {file_result['error']}")
                    results["failed_processing"] += 1
                    
            except Exception as e:
                error_msg = f"Error processing {file_stem}: {str(e)}"
                print(f"❌ {error_msg}")
                results["errors"].append(error_msg)
                results["failed_processing"] += 1
        
        # Print summary
        print(f"\n{'='*60}")
        print("PROCESSING SUMMARY")
        print(f"{'='*60}")
        print(f"Total files processed: {results['total_files_processed']}")
        print(f"Successful: {results['successful_processing']}")
        print(f"Failed: {results['failed_processing']}")
        print(f"CSV files without JSON: {len(results['csv_only_files'])}")
        print(f"JSON files without CSV: {len(results['json_only_files'])}")
        
        if results["errors"]:
            print(f"\nErrors encountered:")
            for error in results["errors"]:
                print(f"  - {error}")
        
        return results
        
    except Exception as e:
        results["status"] = "error"
        results["errors"].append(f"Flow error: {str(e)}")
        return results


def main():
    """Main function for testing the integrated flow."""
    # Example usage
    csv_dir = Path("vgb_training_data/gt")
    json_dir = Path("vgb_training_data/input_labels")
    output_dir = Path("output")
    
    # Option 1: Use template for all files
    template_path = Path("templates/adjusted/dyanmic_table_template.pdf.labels.json")
    
    # Option 2: Use corresponding JSON files
    # template_path = None
    
    result = integrated_label_population_flow(
        csv_dir=csv_dir,
        json_dir=json_dir,
        output_dir=output_dir,
        template_json_path=template_path
    )
    
    print(f"\nFinal result: {result['status']}")


if __name__ == "__main__":
    main() 