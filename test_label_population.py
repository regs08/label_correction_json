from pathlib import Path
from src.flows.label_population_flow import label_population_flow
from src.utils.config import Settings

def main():
    # Get the current working directory
    cwd = Path.cwd()
    
    # Define paths - use direct Path objects for absolute paths
    input_csv = Path("/Users/nr466/Python Projects/label_correction_json/synthetic_data/ground_truth/syn_mixed01.csv")
    input_json = Path("/Users/nr466/Python Projects/label_correction_json/synthetic_data/syn_templates/syn_template01.pdf.labels.json")
    
    # Generate output filename from CSV filename
    output_json = cwd / "output" / f"{input_csv.stem.replace('_gt_', '').lower()}.pdf.labels.json"
    
    # Create settings
    settings = Settings(
        input_csv_path=input_csv,
        input_json_path=input_json,
        output_json_path=output_json
    )
    
    # Run the flow
    print("Starting label population flow...")
    result = label_population_flow(
        input_csv_path=input_csv,
        input_json_path=input_json,
        output_json_path=output_json,
        settings=settings
    )
    
    # Print results
    print("\nProcessing Results:")
    print(f"Status: {result['status']}")
    if result['status'] == 'success':
        print(f"Changes made: {result['changes_made']}")
        print(f"Output saved to: {result['output_path']}")
        print("\nReport:")
        for key, value in result['report'].items():
            print(f"{key}: {value}")
    else:
        print(f"Error: {result['error']}")

if __name__ == "__main__":
    main() 

#Focus on creating a script that will have templates that we can just plug our gt csvs into. it will populate a given template and 