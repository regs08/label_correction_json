from pathlib import Path
import sys
from os.path import dirname, abspath

# Add the src directory to the Python path
sys.path.append(dirname(dirname(abspath(__file__))))

from flows.label_correction_flow import single_file_correction_flow

def main():
    # Define paths for input and output files
    base_dir = Path("data")
    
    # Input files
    ground_truth_path = "/Users/nr466/Python Projects/correct_labels_json/bates_060325_008.csv"
    labels_json_path = "/Users/nr466/Python Projects/correct_labels_json/bates_060325_0007.pdf.labels.json"
    
    # Output files
    output_dir = base_dir / "output"
    output_json_path = output_dir / "corrected_labels.json"
    correction_report_path = output_dir / "correction_report.csv"
    
    # Create output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Run the correction flow
    print("Starting label correction flow...")
    single_file_correction_flow(
        ground_truth_path=ground_truth_path,
        labels_json_path=labels_json_path,
        output_json_path=output_json_path,
        correction_report_path=correction_report_path
    )
    print("Label correction completed!")
    print(f"Corrected JSON saved to: {output_json_path}")
    print(f"Correction report saved to: {correction_report_path}")

if __name__ == "__main__":
    main() 