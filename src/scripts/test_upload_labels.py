from pathlib import Path
from src.flows.upload_labels_flow import upload_labels_flow
from src.utils.config import UploadLabelsConfig

def main():
    # Create upload config
    config = UploadLabelsConfig(
        source_folder=Path("data/session/checkpoints/ckpt3_corrected_labels"),
    )
    
    # Run upload flow
    print("Starting label upload flow...")
    print(f"Source directory: {config.source_folder}")
    print(f"Using prefix: {config.prefix}")
    print(f"File pattern: {config.file_pattern}")
    print("\nRunning flow...")
    
    result = upload_labels_flow(config)
    
    # Print results
    print("\nResults:")
    print(f"Total files uploaded: {result['total_files']}")
    if result['uploaded_files']:
        print("\nUploaded files:")
        for blob_name in result['uploaded_files']:
            print(f"- {blob_name}")

if __name__ == '__main__':
    main() 