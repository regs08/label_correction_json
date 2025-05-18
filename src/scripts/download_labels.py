from pathlib import Path
from src.flows.parent_pipeline import parent_pipeline
from src.utils.config import (
    PipelineConfig, 
    AzureDownloadConfig, 
    SetupConfig, 
    FileMatchConfig,
    CompareFilesConfig
)

def main():
    # Default values
    working_folder = "data"
    prefix = None
    
    # Create configs
    setup_config = SetupConfig(
        working_folder=working_folder
    )
    
    azure_download_config = AzureDownloadConfig(
        prefix=prefix,
        output_dir=str(setup_config.get_checkpoints_path() / "ckpt1_downloaded_labels")
    )
    
    # Create file match config
    file_match_config = FileMatchConfig(
        source_folder=Path(azure_download_config.output_dir),
        ground_truth_folder=setup_config.get_ground_truth_path(),
        output_folder=setup_config.get_checkpoints_path() / "ckpt2_matched_files"
    )
    
    # Create compare files config
    compare_files_config = CompareFilesConfig(
        matched_files=[],  # Will be populated from file match results
        output_folder=setup_config.get_checkpoints_path() / "ckpt3_corrected_labels",
        report_folder=setup_config.get_checkpoints_path() / "ckpt3_correction_reports"
    )
    
    # Create pipeline config
    pipeline_config = PipelineConfig(
        setup=setup_config,
        azure_download=azure_download_config,
        file_match=file_match_config,
        compare_files=compare_files_config
    )
    
    # Run parent pipeline
    result = parent_pipeline(config=pipeline_config)
    
    # Print setup summary
    print(f"\nSetup Summary:")
    print(f"Session folder: {result['setup']['session_path']}")
    print(f"Checkpoints folder: {result['setup']['checkpoints_path']}")
    print(f"Ground truth folder: {result['setup']['ground_truth_path']}")
    
    # Print download summary
    print(f"\nDownload Summary:")
    print(f"Total files downloaded: {result['download']['total_files']}")
    print(f"Files saved to: {pipeline_config.azure_download.output_dir}")
    print("\nDownloaded files:")
    for path in result['download']['local_paths']:
        print(f"- {path}")
    
    # Print file match summary
    if result['file_match']:
        print(f"\nFile Match Summary:")
        print(f"Total matches found: {len(result['file_match']['matched_files'])}")
        print("\nMatched file pairs:")
        for source_path, ground_truth_path in result['file_match']['matched_files']:
            print(f"Source: {source_path}")
            print(f"Ground Truth: {ground_truth_path}")
            print("---")
    
    # Print label correction summary
    if result['label_correction']:
        print(f"\nLabel Correction Summary:")
        print(f"Total files processed: {result['label_correction']['total_files_processed']}")
        print(f"Total files corrected: {result['label_correction']['total_files_corrected']}")
        print(f"Total reports generated: {result['label_correction']['total_reports_generated']}")
        
        if result['label_correction']['corrected_files']:
            print("\nCorrected files:")
            for path in result['label_correction']['corrected_files']:
                print(f"- {path}")
        
        if result['label_correction']['report_files']:
            print("\nCorrection reports:")
            for path in result['label_correction']['report_files']:
                print(f"- {path}")
    else:
        print("\nNo label corrections were performed.")

if __name__ == '__main__':
    main() 