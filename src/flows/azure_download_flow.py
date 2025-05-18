from prefect import flow
from typing import Dict, List
from pathlib import Path
from prefect import get_run_logger

from src.tasks.azure_tasks import list_label_files, download_label_file, save_label_file_locally
from src.utils.config import AzureDownloadConfig

@flow(name="azure_download_flow")
def azure_download_flow(config: AzureDownloadConfig) -> Dict[str, List[str]]:
    """
    Download label files from Azure Blob Storage and save them locally.

    Args:
        config: AzureDownloadConfig containing flow parameters

    Returns:
        Dictionary with summary of processed files
    """
    logger = get_run_logger()
    logger.info("Starting Azure download flow")
    
    output_path = Path(config.output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    logger.info(f"Output directory: {output_path.absolute()}")

    # List label files
    logger.info("Listing label files...")
    label_files = list_label_files(prefix=config.prefix)
    logger.info(f"Found {len(label_files)} label files")

    if not label_files:
        logger.warning("No label files found to download")
        return {
            "total_files": 0,
            "local_paths": []
        }

    local_paths = []
    for i, blob_name in enumerate(label_files, 1):
        logger.info(f"Processing file {i}/{len(label_files)}: {blob_name}")
        try:
            label_data = download_label_file(blob_name)
            local_path = save_label_file_locally(label_data, blob_name, checkpoint_dir=str(output_path))
            local_paths.append(local_path)
            logger.info(f"Successfully saved to {local_path}")
        except Exception as e:
            logger.error(f"Error processing {blob_name}: {str(e)}")
            continue

    logger.info(f"Downloaded {len(local_paths)} files to {output_path.absolute()}")
    return {
        "total_files": len(local_paths),
        "local_paths": local_paths
    }
