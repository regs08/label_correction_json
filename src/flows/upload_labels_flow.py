from prefect import flow
from pathlib import Path
from typing import Dict, List
from prefect import get_run_logger
from src.tasks.azure_tasks import upload_label_files
from src.utils.config import UploadLabelsConfig

@flow(name="upload_labels_flow")
def upload_labels_flow(config: UploadLabelsConfig) -> Dict[str, List[str]]:
    """
    Upload all label files from a directory to Azure blob storage.
    
    Args:
        config: UploadLabelsConfig containing upload parameters
        
    Returns:
        Dictionary containing:
        - uploaded_files: List of successfully uploaded blob names
        - total_files: Number of files processed
    """
    logger = get_run_logger()
    logger.info(f"Starting upload from directory: {config.source_folder}")
    
    try:
        # Get all matching files
        label_files = config.get_source_files()
        logger.info(f"Found {len(label_files)} label files to upload")
        
        if not label_files:
            logger.warning("No label files found to upload")
            return {
                "uploaded_files": [],
                "total_files": 0
            }
        
        # Convert paths to strings for the upload task
        file_paths = [str(path) for path in label_files]
        
        # Upload files
        logger.info("Uploading files to Azure...")
        uploaded_blobs = upload_label_files(file_paths, prefix=config.prefix)
        logger.info(f"Successfully uploaded {len(uploaded_blobs)} files")
        
        return {
            "uploaded_files": uploaded_blobs,
            "total_files": len(uploaded_blobs)
        }
        
    except Exception as e:
        logger.error(f"Error during upload: {str(e)}")
        raise 