from prefect import flow
from src.flows.setup_flow import setup_flow
from src.flows.azure_download_flow import azure_download_flow
from src.flows.file_match_flow import file_match_flow
from src.flows.label_correction_flow import label_correction_flow
from src.flows.upload_labels_flow import upload_labels_flow
from src.utils.config import PipelineConfig, UploadLabelsConfig
from pathlib import Path
from prefect import get_run_logger

@flow(name="parent_pipeline")
def parent_pipeline(config: PipelineConfig) -> dict:
    """
    Parent pipeline that orchestrates the setup, download, file matching, and label correction flows.
    
    Args:
        config: PipelineConfig containing all necessary configurations
        
    Returns:
        Dictionary with results from all flows
    """
    logger = get_run_logger()
    logger.info("Starting parent pipeline")
    
    # Run setup flow
    logger.info("Running setup flow...")
    setup_result = setup_flow(config=config.setup)
    logger.info("Setup flow completed")
    
    # Run download flow
    logger.info("Running download flow...")
    download_result = azure_download_flow(config=config.azure_download)
    logger.info("Download flow completed")
    
    # Run file match flow if config is provided
    file_match_result = None
    label_correction_result = None
    upload_result = None
    
    if config.file_match is not None:
        logger.info("Running file match flow...")
        try:
            file_match_result = file_match_flow(config=config.file_match)
            logger.info("File match flow completed")
            
            # If we have matched files, update compare config and run label correction
            if file_match_result and file_match_result.get("matched_files"):
                logger.info(f"Found {len(file_match_result['matched_files'])} matched files")
                # Convert matched files to (json_path, csv_path) tuples
                matched_pairs = [
                    (Path(source), Path(ground_truth))
                    for source, ground_truth in file_match_result["matched_files"]
                ]
                
                # Update compare config with matched pairs
                config.compare_files.matched_files = matched_pairs
                
                # Run label correction flow
                logger.info("Running label correction flow...")
                label_correction_result = label_correction_flow(config=config.compare_files)
                logger.info("Label correction flow completed")
                
                # If we have corrected files, run upload flow
                if label_correction_result and label_correction_result.get("corrected_files"):
                    logger.info(f"Found {len(label_correction_result['corrected_files'])} corrected files to upload")
                    
                    # Create upload config if not provided
                    if config.upload_labels is None:
                        config.upload_labels = UploadLabelsConfig(
                            source_folder=config.compare_files.output_folder or Path("data/session/checkpoints/ckpt3_corrected_labels"),
                            prefix="scouting_sheets"  # No prefix - upload directly to container root
                        )
                    
                    # Run upload flow
                    logger.info("Running upload labels flow...")
                    upload_result = upload_labels_flow(config=config.upload_labels)
                    logger.info("Upload labels flow completed")
                else:
                    logger.info("No corrected files to upload")
            else:
                logger.info("No matched files found")
        except Exception as e:
            logger.error(f"Error in pipeline: {str(e)}")
            raise
    else:
        logger.info("No file match config provided")
    
    return {
        "setup": setup_result,
        "download": download_result,
        "file_match": file_match_result,
        "label_correction": label_correction_result,
        "upload": upload_result
    }
