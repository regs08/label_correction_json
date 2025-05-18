from pathlib import Path
from src.flows.file_match_flow import file_match_flow
from src.utils.config import FileMatchConfig

def main():
    # Define paths
    source_dir = Path("data/session/checkpoints/ckpt1_downloaded_labels")
    ground_truth_dir = Path("data/ground_truth")
    output_dir = Path("data/session/checkpoints/ckpt2_matched_files")
    
    # Create config
    config = FileMatchConfig(
        source_folder=source_dir,
        ground_truth_folder=ground_truth_dir,
        output_folder=output_dir
    )
    
    # Run file match flow
    print("Starting file match flow test...")
    print(f"Source directory: {source_dir}")
    print(f"Ground truth directory: {ground_truth_dir}")
    print(f"Output directory: {output_dir}")
    print("\nRunning flow...")
    
    result = file_match_flow(config)
    
    # Print results
    print("\nResults:")
    print(f"Total matches found: {len(result['matched_files'])}")
    if result['matched_files']:
        print("\nMatched file pairs:")
        for source_path, ground_truth_path in result['matched_files']:
            print(f"Source: {source_path}")
            print(f"Ground Truth: {ground_truth_path}")
            print("---")

if __name__ == '__main__':
    main() 