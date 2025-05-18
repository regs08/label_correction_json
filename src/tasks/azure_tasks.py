from prefect import task
from typing import List, Dict, Any, Optional
from pathlib import Path
import json

from src.utils.azure_storage import AzureStorageClient
from src.utils.config import get_settings

@task(name="list_label_files", retries=3, retry_delay_seconds=5)
def list_label_files(prefix: Optional[str] = None) -> List[str]:
    """
    List all labels.json files in the source container.
    
    Args:
        prefix: Optional prefix to filter blob names
        
    Returns:
        List of blob names for label files
    """
    azure_client = AzureStorageClient()
    settings = get_settings()
    
    all_blobs = azure_client.list_blobs(settings.azure_source_container_name, prefix)
    
    # Filter for only JSON files or specific filename patterns
    label_files = [blob for blob in all_blobs if blob.endswith('labels.json')]
    
    return label_files

@task(name="download_label_file", retries=3, retry_delay_seconds=5)
def download_label_file(blob_name: str) -> Dict[str, Any]:
    """
    Download a specific labels.json file from Azure.
    
    Args:
        blob_name: The name of the blob to download
        
    Returns:
        The parsed JSON content of the file with headers removed
    """
    azure_client = AzureStorageClient()
    settings = get_settings()
    
    data = azure_client.download_json_blob(settings.azure_source_container_name, blob_name)
    

    
    return data

@task(name="save_label_file_locally", retries=2)
def save_label_file_locally(
    label_data: Dict[str, Any],
    blob_name: str,
    checkpoint_dir: Optional[str] = None
) -> str:
    """
    Save downloaded label data to a local file.
    
    Args:
        label_data: The label data to save
        blob_name: Name of the blob file
        checkpoint_dir: Optional checkpoint directory to save the file in.
                       If not provided, will save in data/temp/scouting_sheets.
    
    Returns:
        str: Path to the saved file
    """
    # Create base directory if not using checkpoint
    if checkpoint_dir is None:
        base_dir = Path("data/temp/scouting_sheets")
    else:
        base_dir = Path(checkpoint_dir)
    
    # Ensure directory exists
    base_dir.mkdir(parents=True, exist_ok=True)
    print(f"Saving to directory: {base_dir.absolute()}")
    
    # Extract just the filename from the blob path
    # This prevents creating unnecessary subdirectories
    file_name = Path(blob_name).name
    
    # Create the full path for the file
    file_path = base_dir / file_name
    print(f"Full file path: {file_path.absolute()}")
    
    # Save the file
    with open(file_path, 'w') as f:
        json.dump(label_data, f, indent=2)
    
    return str(file_path)

@task(name="upload_corrected_label_file", retries=3, retry_delay_seconds=10)
def upload_corrected_label_file(corrected_data: Dict[str, Any], blob_name: str) -> None:
    """
    Upload a corrected labels.json file back to Azure.
    
    Args:
        corrected_data: The corrected JSON data
        blob_name: The name of the blob to upload to
    """
    azure_client = AzureStorageClient()
    settings = get_settings()
    
    azure_client.upload_json_blob(settings.azure_destination_container_name, blob_name, corrected_data)

@task(name="upload_label_file", retries=3, retry_delay_seconds=5)
def upload_label_file(file_path: str, blob_name: Optional[str] = None) -> str:
    """
    Upload a single label file to Azure blob storage.
    
    Args:
        file_path: Path to the local file to upload
        blob_name: Optional name for the blob. If not provided, uses the filename
        
    Returns:
        The name of the uploaded blob
    """
    azure_client = AzureStorageClient()
    settings = get_settings()
    
    # If no blob name provided, use the filename
    if blob_name is None:
        blob_name = Path(file_path).name
    
    # Read the file
    with open(file_path, 'r') as f:
        data = json.load(f)
    
    # Upload to Azure with overwrite=True
    azure_client.upload_json_blob(
        settings.azure_destination_container_name,
        blob_name,
        data,
        overwrite=True  # Always overwrite existing files
    )
    
    return blob_name

@task(name="upload_label_files", retries=3, retry_delay_seconds=5)
def upload_label_files(file_paths: List[str], prefix: Optional[str] = None) -> List[str]:
    """
    Upload multiple label files to Azure blob storage.
    
    Args:
        file_paths: List of paths to local files to upload
        prefix: Optional prefix to add to blob names
        
    Returns:
        List of uploaded blob names
    """
    uploaded_blobs = []
    
    for file_path in file_paths:
        # Create blob name with optional prefix
        blob_name = Path(file_path).name
        if prefix:
            blob_name = f"{prefix}/{blob_name}"
        
        # Upload the file
        uploaded_blob = upload_label_file(file_path, blob_name)
        uploaded_blobs.append(uploaded_blob)
    
    return uploaded_blobs 